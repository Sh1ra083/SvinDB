#!/usr/bin/env python3
"""Сетевые злоупотребления: узел обязан продолжать обслуживать остальных.

Проверяется не «отвечает ли он на плохое», а **остаётся ли он доступен для
хорошего**, пока плохое происходит. Это и есть отказ в обслуживании: не падение
процесса, а то, что честный клиент перестаёт получать ответы.
"""
import socket, threading, time, http.client, json, subprocess, os, sys

PORT = int(os.environ.get("NPORT", "19497"))
# На macOS /private/tmp существует, но Linux-стенд работает с обычным /tmp.
# Каталог должен задаваться явно для каждого прогона: harness не имеет права
# удалять произвольный рабочий путь и не должен пересекаться с production.
DIR = os.environ.get("NDIR", "/tmp/sv-net")
BIN = os.environ.get("NBIN", "/tmp/svindb-h4")


def boot():
    subprocess.run(["pkill", "-f", f"serve {DIR} "], capture_output=True)
    time.sleep(0.8)
    subprocess.run(["rm", "-rf", DIR], capture_output=True)
    os.makedirs(DIR, exist_ok=True)
    p = subprocess.Popen(
        [BIN, "serve", DIR, str(PORT), "--single-index", "--fields=title:2,body:1"],
                         stdout=open("/tmp/net-node.log", "w"), stderr=subprocess.STDOUT,
                         start_new_session=True)
    for _ in range(60):
        try:
            healthy(3); return p
        except Exception:
            time.sleep(0.25)
    return None


def healthy(to=5):
    c = http.client.HTTPConnection("localhost", PORT, timeout=to)
    c.request("GET", "/_health"); r = c.getresponse(); r.read(); c.close()
    return True


def seed():
    c = http.client.HTTPConnection("localhost", PORT, timeout=60)
    docs = {"documents": [{"id": f"d{i}", "title": f"t{i}", "body": "общее"} for i in range(500)]}
    c.request("POST", "/_bulk", json.dumps(docs).encode(), {"Content-Type": "application/json"})
    c.getresponse().read(); c.close()
    c = http.client.HTTPConnection("localhost", PORT, timeout=60)
    c.request("POST", "/_flush", b"{}", {"Content-Type": "application/json"})
    c.getresponse().read(); c.close()
    time.sleep(1.2)


def served(to=5):
    """Отвечает ли узел честному клиенту прямо сейчас."""
    t0 = time.perf_counter()
    try:
        c = http.client.HTTPConnection("localhost", PORT, timeout=to)
        c.request("POST", "/", json.dumps({"k": 3, "words": [{"field": 1, "terms": ["общее"]}]}).encode(),
                  {"Content-Type": "application/json"})
        r = c.getresponse(); r.read(); c.close()
        return True, time.perf_counter() - t0
    except Exception as e:
        return False, time.perf_counter() - t0


def slowloris(n, hold, stop):
    socks = []
    for _ in range(n):
        try:
            s = socket.create_connection(("localhost", PORT), timeout=5)
            s.sendall(b"POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 1000000\r\n\r\n")
            socks.append(s)
        except Exception:
            break
    t0 = time.time()
    while not stop.is_set() and time.time() - t0 < hold:
        for s in socks:
            try:
                s.sendall(b"a")
            except Exception:
                pass
        time.sleep(0.5)
    for s in socks:
        try:
            s.close()
        except Exception:
            pass
    return len(socks)


def idle_holds(n, hold, stop):
    socks = []
    for _ in range(n):
        try:
            s = socket.create_connection(("localhost", PORT), timeout=5)
            s.sendall(b"GET / HTTP/1.1\r\n")   # заголовок не дописан
            socks.append(s)
        except Exception:
            break
    t0 = time.time()
    while not stop.is_set() and time.time() - t0 < hold:
        time.sleep(0.3)
    for s in socks:
        try:
            s.close()
        except Exception:
            pass
    return len(socks)


def main():
    p = boot()
    if not p:
        print("узел не поднялся"); return 1
    seed()
    ok, dt = served()
    print(f"до нагрузки: обслуживает={ok}, {dt * 1000:.1f} мс\n")

    for name, fn, n in (("медленные тела (slowloris)", slowloris, 200),
                        ("недописанные заголовки", idle_holds, 200)):
        stop = threading.Event()
        held = [0]
        t = threading.Thread(target=lambda: held.__setitem__(0, fn(n, 12, stop)), daemon=True)
        t.start()
        time.sleep(3)
        results = [served(6) for _ in range(5)]
        stop.set(); t.join(timeout=20)
        good = sum(1 for o, _ in results if o)
        worst = max(d for _, d in results)
        alive = p.poll() is None
        mark = "·" if good >= 4 and alive else "✗"
        print(f"  {mark} {name:30} удержано {held[0]:4}, честных ответов {good}/5, "
              f"худший {worst * 1000:7.1f} мс, узел {'жив' if alive else 'УМЕР'}")

    # много быстрых соединений подряд
    t0 = time.perf_counter()
    fails = 0
    for _ in range(500):
        try:
            s = socket.create_connection(("localhost", PORT), timeout=3)
            s.close()
        except Exception:
            fails += 1
    print(f"  · 500 соединений подряд          отказов {fails}, за {time.perf_counter() - t0:.1f} с")

    ok, dt = served()
    print(f"\nпосле нагрузки: обслуживает={ok}, {dt * 1000:.1f} мс, узел "
          f"{'жив' if p.poll() is None else 'УМЕР'}")
    subprocess.run(["pkill", "-f", f"serve {DIR} "], capture_output=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
