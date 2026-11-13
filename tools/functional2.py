#!/usr/bin/env python3
"""Второй функциональный прогон: то, что не покрыто первым.

Здесь всё, что живёт не внутри одного запроса, — снимки, шарды, векторы,
восстановление после убийства, права, обслуживание, целость под нагрузкой.
Такие вещи ломаются не в логике, а на стыках, и микротестами ядра не ловятся.
"""
import http.client, json, subprocess, sys, time, os, shutil, signal, random, threading

BIN = os.environ.get("FBIN", "/tmp/svindb-b5")
ok = fail = 0
failures = []

def call(port, method, path, body=None, hdr=None, raw=False):
    c = http.client.HTTPConnection("localhost", port, timeout=300)
    b = body if raw else (json.dumps(body).encode() if body is not None else None)
    h = {"Content-Type": "application/json"}
    if hdr: h.update(hdr)
    c.request(method, path, b, h)
    r = c.getresponse(); data = r.read(); c.close()
    try: return r.status, json.loads(data)
    except Exception: return r.status, data

def check(name, cond, detail=""):
    global ok, fail
    if cond: ok += 1; print(f"  ✓ {name}")
    else:
        fail += 1; failures.append(f"{name}: {detail}"); print(f"  ✗ {name}  {detail}")

def boot(d, port, fields, extra=()):
    subprocess.run(["pkill", "-f", f"serve {d} "], capture_output=True)
    time.sleep(0.7)
    p = subprocess.Popen(
        [BIN, "serve", d, str(port), "--single-index", f"--fields={fields}", *extra],
                         stdout=open(f"/tmp/f2-{port}.log", "w"), stderr=subprocess.STDOUT,
                         start_new_session=True)
    for _ in range(80):
        try:
            if call(port, "GET", "/_health")[0] == 200: return p
        except Exception: pass
        time.sleep(0.25)
    return None

def hits(r): return r[1].get("hits", []) if isinstance(r[1], dict) else []
def hid(r):  return [h.get("id") for h in hits(r)]

FIELDS = "title:2,body:1,цена:int64"

def docs(n, off=0):
    return [{"id": f"d{i+off}", "title": f"док{i+off}",
             "body": f"тело общее слово{(i+off) % 7}", "цена": (i + off) % 1000}
            for i in range(n)]

# --------------------------------------------------------------- 1. снимки
def t_snapshot():
    print("\n== снимки и восстановление")
    d, port, dest = "/tmp/f2-snap", 19430, "/tmp/f2-snapdest"
    shutil.rmtree(d, ignore_errors=True); shutil.rmtree(dest, ignore_errors=True)
    os.makedirs(d); os.makedirs(dest)
    cfg = f"/tmp/f2-snap.json"
    open(cfg, "w").write(json.dumps({"snapshot": {"allow": [dest]}}))
    if not boot(d, port, FIELDS, ["--config=" + cfg]):
        return check("узел со снимками поднялся", False, open(f"/tmp/f2-{port}.log").read()[-300:])
    call(port, "POST", "/_bulk", {"documents": docs(500)})
    call(port, "POST", "/_flush", {})
    time.sleep(1)
    # Имя снимка становится именем каталога, поэтому только ASCII.
    name = "nightly"
    st, r = call(port, "POST", f"/_snapshot/{name}", {"dest": dest})
    check("снимок снят", st < 300, f"{st} {r}")
    st, r = call(port, "GET", "/_snapshot")
    check("снимок виден в списке", name in str(r), str(r)[:200])

    back = "/private/tmp/f2-restored"
    shutil.rmtree(back, ignore_errors=True)
    arch = os.path.join(dest, name)
    p = subprocess.run([BIN, "restore", arch, back], capture_output=True, text=True)
    check("восстановление прошло", p.returncode == 0, (p.stdout + p.stderr)[-300:])
    p = subprocess.run([BIN, "check", back, "--full"], capture_output=True, text=True)
    check("восстановленный индекс цел", p.returncode == 0, (p.stdout + p.stderr)[-400:])
    port2 = 19431
    if boot(back, port2, FIELDS):
        st, h = call(port2, "GET", "/_health")
        check("в восстановленном те же 500", h.get("docs") == 500, str(h)[:200])
        r = call(port2, "POST", "/", {"k": 5, "words": [{"field": "body", "terms": ["общее"]}],
                                      "ids": True})
        check("поиск по восстановленному работает", len(hits(r)) == 5, str(r[1])[:150])
        subprocess.run(["pkill", "-f", f"serve {back} "], capture_output=True)
    subprocess.run(["pkill", "-f", f"serve {d} "], capture_output=True)

# ------------------------------------------------------- 2. журнал и убийство
def t_wal():
    print("\n== журнал приёма: убийство посреди работы")
    d, port = "/tmp/f2-wal", 19432
    shutil.rmtree(d, ignore_errors=True); os.makedirs(d)
    p = boot(d, port, FIELDS)
    if not p: return check("узел поднялся", False)
    # Кладём без сброса: всё живёт только в журнале и буфере.
    call(port, "POST", "/_bulk", {"documents": docs(300)})
    time.sleep(0.3)
    os.killpg(os.getpgid(p.pid), signal.SIGKILL)
    time.sleep(1)
    if not boot(d, port, FIELDS):
        return check("узел поднялся после убийства", False,
                     open(f"/tmp/f2-{port}.log").read()[-400:])
    # Восстановленное лежит в буфере: журнал вернул его туда, а не в сегмент.
    # Ждём сброса — своим временем, как это делает свежесть.
    call(port, "POST", "/_flush", {})
    for _ in range(40):
        st, h = call(port, "GET", "/_health")
        if h.get("docs") == 300: break
        time.sleep(0.25)
    check("журнал вернул все 300", h.get("docs") == 300, str(h)[:250])
    r = call(port, "POST", "/", {"k": 3, "words": [{"field": "body", "terms": ["общее"]}],
                                 "ids": True})
    check("восстановленное ищется", len(hits(r)) == 3, str(r[1])[:150])
    subprocess.run(["pkill", "-f", f"serve {d} "], capture_output=True)

# ------------------------------------------------------------- 3. шарды
def t_shards():
    print("\n== шарды по сети: координатор")
    ports, dirs = [19440, 19441, 19442], []
    for i, p in enumerate(ports):
        dd = f"/tmp/f2-shard{i}"
        shutil.rmtree(dd, ignore_errors=True); os.makedirs(dd); dirs.append(dd)
        if not boot(dd, p, FIELDS):
            return check(f"шард {i} поднялся", False)
    # Документы раскладываются по шардам тем же правилом, что и у координатора.
    for i, p in enumerate(ports):
        call(p, "POST", "/_bulk", {"documents": docs(200, off=i * 1000)})
        call(p, "POST", "/_flush", {})
    time.sleep(1.5)
    cport = 19443
    cdir = "/tmp/f2-coord"
    shutil.rmtree(cdir, ignore_errors=True); os.makedirs(cdir)
    subprocess.Popen([BIN, "serve", cdir, str(cport), "--single-index",
                      "--shards=" + ",".join(f"localhost:{p}" for p in ports)],
                     stdout=open(f"/tmp/f2-{cport}.log", "w"), stderr=subprocess.STDOUT,
                     start_new_session=True)
    time.sleep(2)
    q = {"k": 10, "words": [{"field": "body", "terms": ["общее"]}]}
    st, r = call(cport, "POST", "/", q)
    check("координатор ответил", st == 200 and len(r.get("hits", [])) == 10, f"{st} {str(r)[:200]}")
    st, g = call(cport, "POST", "/", dict(q, scoring="global"))
    check("точные скоры двумя кругами", st == 200 and len(g.get("hits", [])) == 10,
          f"{st} {str(g)[:200]}")
    # Один шард на все документы — эталон веса.
    one = "/tmp/f2-sole"; shutil.rmtree(one, ignore_errors=True); os.makedirs(one)
    if boot(one, 19444, FIELDS):
        for i in range(3):
            call(19444, "POST", "/_bulk", {"documents": docs(200, off=i * 1000)})
        call(19444, "POST", "/_flush", {}); time.sleep(1)
        st, o = call(19444, "POST", "/", q)
        gs = [round(h["score"], 9) for h in g.get("hits", [])]
        os_ = [round(h["score"], 9) for h in o.get("hits", [])]
        check("веса при scoring=global совпали с одним индексом", gs == os_, f"{gs[:3]} / {os_[:3]}")
        subprocess.run(["pkill", "-f", f"serve {one} "], capture_output=True)
    for dd in dirs + [cdir]:
        subprocess.run(["pkill", "-f", f"serve {dd} "], capture_output=True)

# ------------------------------------------------------------- 4. векторы
def t_vector():
    print("\n== плотный вектор")
    d, port = "/tmp/f2-vec", 19445
    shutil.rmtree(d, ignore_errors=True); os.makedirs(d)
    if not boot(d, port, "title:2,v:dense_vector:4"):
        return check("узел с вектором поднялся", False,
                     open(f"/tmp/f2-{port}.log").read()[-300:])
    vs = [[1, 0, 0, 0], [0, 1, 0, 0], [0.9, 0.1, 0, 0], [0, 0, 1, 0]]
    call(port, "POST", "/_bulk", {"documents": [
        {"id": f"v{i}", "title": f"век{i}", "v": " ".join(map(str, v))} for i, v in enumerate(vs)]})
    call(port, "POST", "/_flush", {}); time.sleep(1)
    st, r = call(port, "POST", "/", {"k": 2, "vector": {"field": "v", "query": [1, 0, 0, 0]},
                                     "ids": True})
    got = [h.get("id") for h in r.get("hits", [])] if isinstance(r, dict) else []
    check("ближайшие — v0 и v2", got == ["v0", "v2"], f"{st} {got}")
    subprocess.run(["pkill", "-f", f"serve {d} "], capture_output=True)

# ------------------------------------------------------------- 5. права
def t_security():
    print("\n== права доступа")
    d, port = "/tmp/f2-sec", 19446
    shutil.rmtree(d, ignore_errors=True); os.makedirs(d)
    ITER = 210000
    p = subprocess.run([BIN, "passwd", str(ITER)], input="секрет\nсекрет\n",
                       capture_output=True, text=True)
    h = [l for l in p.stdout.splitlines() if l.strip().startswith("pbkdf2")]
    if not h:
        return check("passwd выдал хеш", False, (p.stdout + p.stderr)[-300:])
    cfg = "/tmp/f2-sec.json"
    open(cfg, "w").write(json.dumps({"auth": {
        "iterations": ITER,
        "roles": [{"name": "всё", "privileges": ["admin", "write", "read"]}],
        "users": [{"name": "admin", "hash": h[-1], "roles": ["всё"]}]}}))
    if not boot(d, port, FIELDS, ["--config=" + cfg]):
        return check("узел с правами поднялся", False, open(f"/tmp/f2-{port}.log").read()[-400:])
    import base64
    good = {"Authorization": "Basic " + base64.b64encode(b"admin:\xd1\x81\xd0\xb5\xd0\xba\xd1\x80\xd0\xb5\xd1\x82").decode()}
    bad = {"Authorization": "Basic " + base64.b64encode(b"admin:not").decode()}
    st, _ = call(port, "POST", "/_bulk", {"documents": docs(3)})
    check("без ключа приём отвергнут", st in (401, 403), str(st))
    st, _ = call(port, "POST", "/_bulk", {"documents": docs(3)}, hdr=bad)
    check("с неверным паролем отвергнут", st in (401, 403), str(st))
    st, r = call(port, "POST", "/_bulk", {"documents": docs(3)}, hdr=good)
    check("с верным паролем принят", st == 200, f"{st} {r}")
    subprocess.run(["pkill", "-f", f"serve {d} "], capture_output=True)

# ------------------------------------------- 6. одновременная запись и чтение
def t_concurrency():
    print("\n== одновременные запись, чтение и удаление")
    d, port = "/tmp/f2-conc", 19447
    shutil.rmtree(d, ignore_errors=True); os.makedirs(d)
    if not boot(d, port, FIELDS, ["--flush=5000"]):
        return check("узел поднялся", False)
    errs, seen = [], []

    def writer(w):
        for b in range(20):
            st, r = call(port, "POST", "/_bulk",
                         {"documents": docs(250, off=w * 100000 + b * 250)})
            if st != 200: errs.append((st, str(r)[:100]))

    def reader():
        for _ in range(120):
            st, r = call(port, "POST", "/", {"k": 5, "words": [
                {"field": "body", "terms": ["общее"]}]})
            if st != 200: errs.append((st, str(r)[:100]))
            seen.append(len(r.get("hits", [])) if isinstance(r, dict) else -1)
            time.sleep(0.01)

    ts = [threading.Thread(target=writer, args=(w,)) for w in range(4)]
    ts += [threading.Thread(target=reader) for _ in range(3)]
    for t in ts: t.start()
    for t in ts: t.join()
    check("ни одной ошибки под нагрузкой", not errs, str(errs[:3]))
    call(port, "POST", "/_flush", {}); time.sleep(1.5)
    st, hh = call(port, "GET", "/_health")
    check("все 20 000 на месте", hh.get("docs") == 20000, str(hh)[:200])
    st, r = call(port, "POST", "/_delete", {"ids": [f"d{i}" for i in range(100)]})
    check("удаление сотни", r.get("deleted") == 100, str(r))
    call(port, "POST", "/_flush", {}); time.sleep(1)
    p = subprocess.run([BIN, "check", d, "--full"], capture_output=True, text=True)
    check("целость после нагрузки", p.returncode == 0, (p.stdout + p.stderr)[-400:])
    subprocess.run(["pkill", "-f", f"serve {d} "], capture_output=True)

# ------------------------------------------------------------- 7. слияние
def t_merge():
    print("\n== слияние и обслуживание")
    d, port = "/tmp/f2-merge", 19448
    shutil.rmtree(d, ignore_errors=True); os.makedirs(d)
    if not boot(d, port, FIELDS, ["--flush=1000", "--no-maintain"]):
        return check("узел поднялся", False)
    for b in range(10):
        call(port, "POST", "/_bulk", {"documents": docs(1000, off=b * 1000)})
    call(port, "POST", "/_flush", {}); time.sleep(1)
    st, h = call(port, "GET", "/_health")
    many = h.get("segments", 0)
    check("сегментов набралось много", many >= 8, str(h)[:150])
    r_before = call(port, "POST", "/", {"k": 10, "words": [{"field": "body", "terms": ["слово3"]}],
                                        "ids": True})
    subprocess.run(["pkill", "-f", f"serve {d} "], capture_output=True); time.sleep(1)
    p = subprocess.run([BIN, "merge", d, "20", "100000"], capture_output=True, text=True)
    check("слияние прошло", p.returncode == 0, (p.stdout + p.stderr)[-300:])
    if boot(d, port, FIELDS):
        st, h = call(port, "GET", "/_health")
        check("сегментов стало меньше", h.get("segments", 99) < many, f"{many} → {h.get('segments')}")
        check("документы не потерялись", h.get("docs") == 10000, str(h)[:150])
        r_after = call(port, "POST", "/", {"k": 10, "words": [{"field": "body", "terms": ["слово3"]}],
                                           "ids": True})
        check("выдача после слияния та же", hid(r_before) == hid(r_after),
              f"{hid(r_before)[:3]} / {hid(r_after)[:3]}")
    subprocess.run(["pkill", "-f", f"serve {d} "], capture_output=True)

if __name__ == "__main__":
    only = sys.argv[1] if len(sys.argv) > 1 else None
    for name, fn in [("snapshot", t_snapshot), ("wal", t_wal), ("shards", t_shards),
                     ("vector", t_vector), ("security", t_security),
                     ("concurrency", t_concurrency), ("merge", t_merge)]:
        if only and only != name: continue
        try:
            fn()
        except Exception as e:
            check(f"{name}: прогон не упал", False, repr(e)[:300])
    print(f"\n=== ИТОГО: {ok} прошло, {fail} упало")
    for f in failures: print("  ! " + f)
    sys.exit(0 if fail == 0 else 1)
