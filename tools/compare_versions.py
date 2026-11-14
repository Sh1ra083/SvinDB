#!/usr/bin/env python3
"""Compare two local release binaries on the same isolated single-node corpus.

Example:
  python3 tools/compare_versions.py --before /path/to/before --after /path/to/after     --output /path/to/external/results.json

Generated reports belong outside the checkout. This is a native single-host
comparison, not an Elasticsearch benchmark or production capacity claim.
"""
import argparse
import concurrent.futures
import hashlib
import http.client
import json
import os
import pathlib
import socket
import subprocess
import tempfile
import threading
import time

def pct(xs, p):
    a = sorted(xs)
    return a[min(len(a) - 1, int((len(a) - 1) * p))] if a else None

class Node:

    def __init__(self, binary, folder):
        self.tmp = tempfile.TemporaryDirectory(prefix='svindb-alpha4-', dir=folder)
        s = socket.socket()
        s.bind(('127.0.0.1', 0))
        self.port = s.getsockname()[1]
        s.close()
        self.log = open(pathlib.Path(self.tmp.name) / 'node.log', 'w+')
        self.proc = subprocess.Popen([binary, 'serve', self.tmp.name + '/data', str(self.port), '--threads=10', '--no-maintain'], env={**os.environ, 'SVINDB_DISK_FLOOR_MB': '1'}, stdout=self.log, stderr=self.log)
        try:
            for _ in range(1000):
                if self.proc.poll() is not None:
                    raise RuntimeError('node exited')
                try:
                    if self.req('GET', '/_health')[0] == 200:
                        break
                except (OSError, http.client.HTTPException):
                    pass
                time.sleep(0.02)
            else:
                raise RuntimeError('node readiness timed out')
            assert self.req('PUT', '/logs', {'mappings': {'dynamic': 'strict', 'properties': {'message': {'type': 'text'}, 'tag': {'type': 'keyword'}, 'ordinal': {'type': 'long'}}}})[0] == 200
            assert self.req('PUT', '/logs/_settings', {'refresh_interval_ms': 0, 'live_max_docs': 0, 'flush_docs': 10000, 'flush_mb': 128, 'wal_group_commit_ms': 3})[0] == 200
        except BaseException:
            self.close()
            raise

    def client(self):
        return http.client.HTTPConnection('127.0.0.1', self.port, timeout=60)

    def req(self, method, path, body=None, client=None):
        own = client is None
        c = client or self.client()
        try:
            raw = body if isinstance(body, str) else json.dumps(body) if body is not None else None
            c.request(method, path, raw, {'Content-Type': 'application/json'})
            r = c.getresponse()
            return (r.status, json.loads(r.read()))
        finally:
            if own:
                c.close()

    def flush(self):
        for _ in range(600):
            status, p = self.req('POST', '/logs/_flush', {})
            if status == 200:
                return
            if status != 202:
                raise RuntimeError((status, p))
            time.sleep(0.05)
        raise RuntimeError('flush timeout')

    def close(self):
        self.proc.terminate()
        try:
            self.proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()
        self.log.close()
        self.tmp.cleanup()

def one(binary, workers, docs, mixed_seconds, folder):
    n = Node(binary, folder)
    try:
        batches = []
        for at in range(0, docs, 1000):
            batches.append(''.join((json.dumps({'index': {'_id': f'd{i}'}}) + '\n' + json.dumps({'message': 'common searchable recurring message service request completed', 'tag': f'user{i % 40000:05d}', 'ordinal': i}) + '\n' for i in range(at, min(at + 1000, docs)))))
        connections = []
        local = threading.local()

        def upload(batch):
            body, count = batch
            if not hasattr(local, 'client'):
                local.client = n.client()
                connections.append(local.client)
            code, p = n.req('POST', '/logs/_bulk', body, local.client)
            if code != 200 or p.get('errors'):
                raise RuntimeError((code, p))
            assert len(p['items']) == count
        start = time.perf_counter()
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
            sizes = (min(1000, docs - at) for at in range(0, docs, 1000))
            list(pool.map(upload, zip(batches, sizes)))
        ack = time.perf_counter() - start
        for c in connections:
            c.close()
        n.flush()
        ready = time.perf_counter() - start
        code, p = n.req('POST', '/logs/_search', {'query': {'match_all': {}}, 'size': 0, 'track_total_hits': True, '_source': False})
        assert code == 200 and p['total'] == {'value': docs, 'relation': 'eq'}, p
        report = {'workers': workers, 'docs': docs, 'ack_seconds': ack, 'ready_seconds': ready, 'docs_per_second': docs / ack, 'final_count': p['total']}
        if mixed_seconds:
            shapes = [{'query': {'match_all': {}}, 'size': 10, '_source': False}, {'query': {'term': {'tag': 'user00042'}}, 'size': 10, '_source': False}, {'query': {'bool': {'must': [{'term': {'message': 'common'}}, {'term': {'message': 'service'}}]}}, 'size': 10, '_source': False}]
            stop = threading.Event()
            barrier = threading.Barrier(9)

            def client_loop(k):
                c = n.client()
                lat = []
                bad = []
                barrier.wait()
                idx = k
                try:
                    while not stop.is_set():
                        start = time.perf_counter()
                        try:
                            code, p = n.req('POST', '/logs/_search', shapes[idx % 3], c)
                            if code != 200:
                                bad.append({'code': code, 'body': p})
                            else:
                                lat.append((time.perf_counter() - start) * 1000)
                        except Exception as e:
                            bad.append(str(e))
                            c.close()
                            c = n.client()
                        idx += 1
                finally:
                    c.close()
                return (lat, bad)
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
                fs = [pool.submit(client_loop, k) for k in range(8)]
                barrier.wait()
                start = time.perf_counter()
                time.sleep(mixed_seconds)
                stop.set()
                parts = [f.result() for f in fs]
                elapsed = time.perf_counter() - start
            lat = [x for a, b in parts for x in a]
            bad = [x for a, b in parts for x in b]
            report['mixed'] = {'clients': 8, 'seconds': elapsed, 'requests': len(lat), 'qps': len(lat) / elapsed, 'p50_ms': pct(lat, 0.5), 'p99_ms': pct(lat, 0.99), 'errors': len(bad), 'first_errors': bad[:3]}
            if bad:
                raise RuntimeError(report)
            for label, query in [('range', {'query': {'range': {'ordinal': {'gte': docs // 10, 'lt': 9 * docs // 10}}}, 'size': 0, 'track_total_hits': True, '_source': False}), ('terms', {'query': {'match_all': {}}, 'size': 0, 'aggs': {'top': {'terms': {'field': 'tag', 'size': 10}}}, '_source': False})]:
                times = []
                serial_client = n.client()
                for j in range(35):
                    start = time.perf_counter()
                    code, p = n.req('POST', '/logs/_search', query, serial_client)
                    t = (time.perf_counter() - start) * 1000
                    assert code == 200, p
                    if label == 'range':
                        assert p['total']['value'] == 9 * docs // 10 - docs // 10, p
                    else:
                        assert len(p['aggs']['top']['buckets']) == min(10, docs), p
                    if j >= 5:
                        times.append(t)
                serial_client.close()
                report[label] = {'p50_ms': pct(times, 0.5), 'p99_ms': pct(times, 0.99)}
        return report
    finally:
        n.close()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--before', required=True)
    ap.add_argument('--after', required=True)
    ap.add_argument('--docs', type=int, default=100000)
    ap.add_argument('--repeats', type=int, default=3)
    ap.add_argument('--seconds', type=float, default=5)
    ap.add_argument('--output', required=True)
    args = ap.parse_args()
    if args.docs <= 0:
        ap.error('--docs must be positive')
    if args.repeats <= 0 or args.seconds <= 0:
        ap.error('--repeats and --seconds must be positive')
    folder = str(pathlib.Path(args.output).parent)
    out = {'setup': {'docs': args.docs, 'batch': 1000, 'refresh_interval_ms': 0, 'live_max_docs': 0, 'maintenance': False, 'server_threads': 10, 'mixed_clients': 8, 'connection_reuse': True, 'repeats': args.repeats, 'host': os.uname().sysname + ' ' + os.uname().machine, 'scope': 'local native release comparison; same source corpus and refresh; no Elasticsearch comparison'}, 'binaries': {}, 'runs': []}
    for label, path in [('before', args.before), ('after', args.after)]:
        out['binaries'][label] = {'path': path, 'sha256': hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()}
    try:
        for rep in range(args.repeats):
            for label in ['before', 'after'] if rep % 2 == 0 else ['after', 'before']:
                for workers in [1, 4, 8]:
                    result = one(out['binaries'][label]['path'], workers, args.docs, args.seconds if workers == 4 else 0, folder)
                    out['runs'].append({'label': label, 'repeat': rep, **result})
                    pathlib.Path(args.output).write_text(json.dumps(out, indent=2))
                    print(json.dumps(out['runs'][-1]), flush=True)
    finally:
        pathlib.Path(args.output).write_text(json.dumps(out, indent=2))
if __name__ == '__main__':
    main()
