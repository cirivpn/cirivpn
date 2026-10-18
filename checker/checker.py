#!/usr/bin/env python3
"""Проверка доступности узлов Цири ВПН.

Читает nodes.json, проверяет каждый узел и записывает status.json
БЕЗ адресов и портов: только название, страна, статус, пинг и аптайм.
Только стандартная библиотека, подходит для Debian 12 (Python 3.11).

Запуск:  python3 checker.py /etc/ciri-status/nodes.json
"""
import asyncio
import json
import os
import ssl
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

BUCKETS = 48            # столбиков в полосе
WINDOW = 24 * 3600      # окно истории, секунд
BUCKET_SEC = WINDOW // BUCKETS
RANK = {"u": 0, "s": 1, "d": 2}


def load_json(path, default=None):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return default


def write_atomic(path, data):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
    os.replace(tmp, path)
    os.chmod(path, 0o644)


async def check_tcp(host, port, timeout):
    start = time.perf_counter()
    try:
        _, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout)
    except Exception:
        return None
    ms = (time.perf_counter() - start) * 1000
    writer.close()
    try:
        await writer.wait_closed()
    except Exception:
        pass
    return ms


def _http_get(url, timeout, verify):
    ctx = None
    if url.startswith("https") and not verify:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    req = urllib.request.Request(url, headers={"User-Agent": "ciri-status/1.0"})
    start = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
            code = r.status
    except urllib.error.HTTPError as e:
        code = e.code
    except Exception:
        return None
    ms = (time.perf_counter() - start) * 1000
    return ms if code < 500 else None


async def check_http(node, timeout):
    scheme = "https" if node.get("tls", True) else "http"
    path = node.get("path", "/")
    url = f"{scheme}://{node['host']}:{node['port']}{path}"
    return await asyncio.to_thread(_http_get, url, timeout, node.get("verify_tls", False))


async def check_node(node, cfg):
    timeout = cfg.get("timeout_s", 4)
    tries = cfg.get("retries", 2) + 1
    best = None
    for _ in range(tries):
        if node.get("check", "tcp") == "http":
            ms = await check_http(node, timeout)
        else:
            ms = await check_tcp(node["host"], int(node["port"]), timeout)
        if ms is not None:
            best = ms if best is None else min(best, ms)
            break
        await asyncio.sleep(1)
    if best is None:
        return "d", None
    return ("s" if best > cfg.get("slow_ms", 400) else "u"), int(round(best))


def build_bars(history, now):
    start = now - WINDOW
    bars = ["n"] * BUCKETS
    for ts, c in history:
        i = min(BUCKETS - 1, int((ts - start) // BUCKET_SEC))
        if i < 0:
            continue
        if bars[i] == "n" or RANK[c] > RANK[bars[i]]:
            bars[i] = c
    # GitHub Actions запускает расписание нерегулярно (паузы бывают 20-30 минут),
    # поэтому одиночную пустую ячейку между двумя известными закрашиваем
    # предыдущим статусом. Более длинные пропуски остаются серыми.
    for i in range(1, BUCKETS - 1):
        if bars[i] == "n" and bars[i - 1] != "n" and bars[i + 1] != "n":
            bars[i] = bars[i - 1]
    return "".join(bars)


def uptime(history):
    if not history:
        return None
    up = sum(1 for _, c in history if c != "d")
    return round(up * 100 / len(history), 1)


def notify(cfg, text):
    token = os.environ.get("TG_BOT_TOKEN")
    chat = os.environ.get("TG_CHAT_ID")
    if not token or not chat or not cfg.get("telegram_notify", False):
        return
    data = urllib.parse.urlencode({"chat_id": chat, "text": text}).encode()
    try:
        urllib.request.urlopen(
            f"https://api.telegram.org/bot{token}/sendMessage", data=data, timeout=10
        ).read()
    except Exception as e:
        print("telegram error:", type(e).__name__, file=sys.stderr)


LABEL = {"u": "работает", "s": "работает медленно", "d": "недоступен"}


async def main(cfg_path):
    cfg = load_json(cfg_path, {})
    # В GitHub Actions список узлов лежит в секрете, а пути задаются переменными окружения
    if os.environ.get("CIRI_NODES"):
        cfg["nodes"] = json.loads(os.environ["CIRI_NODES"])
    if os.environ.get("CIRI_OUTPUT"):
        cfg["output"] = os.environ["CIRI_OUTPUT"]
    if os.environ.get("CIRI_STATE"):
        cfg["state"] = os.environ["CIRI_STATE"]
    if not cfg.get("nodes") or not cfg.get("output"):
        sys.exit("Не заданы узлы (nodes) или путь для status.json (output)")
    nodes = cfg["nodes"]
    state_path = cfg.get("state", "/var/lib/ciri-status/state.json")
    state = load_json(state_path, {})
    now = int(time.time())

    results = await asyncio.gather(*(check_node(n, cfg) for n in nodes))

    out_nodes = []
    for node, (code, ms) in zip(nodes, results):
        nid = node["id"]
        st = state.setdefault(nid, {"history": [], "notified": None})
        st["history"].append([now, code])
        st["history"] = [h for h in st["history"] if h[0] >= now - WINDOW]

        # уведомляем только когда два последних результата совпадают
        last_two = [c for _, c in st["history"][-2:]]
        if len(last_two) == 2 and last_two[0] == last_two[1]:
            cur = last_two[1]
            if st["notified"] is None:
                st["notified"] = cur
            elif cur != st["notified"]:
                st["notified"] = cur
                notify(cfg, f"Узел «{node['name']}»: {LABEL[cur]}")

        out_nodes.append({
            "id": nid,
            "name": node["name"],
            "country": node.get("country", ""),
            "status": {"u": "up", "s": "slow", "d": "down"}[code],
            "latency_ms": ms,
            "uptime_24h": uptime(st["history"]),
            "bars": build_bars(st["history"], now),
        })

    write_atomic(state_path, state)
    write_atomic(cfg["output"], {
        "updated": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "nodes": out_nodes,
    })


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1] if len(sys.argv) > 1 else "nodes.json"))
