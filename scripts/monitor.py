#!/usr/bin/env python3
"""Live view while you inject failures: ledger progress vs Elasticsearch doc count vs Kafka sink lag.  Ctrl-C to quit."""
import glob, os, sqlite3, sys, time, urllib.request, json, subprocess
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
scenario = sys.argv[1] if len(sys.argv) > 1 else "otel"
index = f"{scenario}-logs"

def ledger():
    g = a = 0
    for f in glob.glob(os.path.join(ROOT, "data", "ledger", "ledger-*.db")):
        try:
            db = sqlite3.connect(f"file:{f}?mode=ro", uri=True, timeout=1)
            r = db.execute("SELECT COUNT(*), COALESCE(SUM(acked),0) FROM events").fetchone(); db.close()
            g += r[0]; a += r[1]
        except Exception:
            pass
    return g, a

def es():
    try:
        return json.loads(urllib.request.urlopen(f"http://localhost:9200/{index}/_count", timeout=3).read())["count"]
    except Exception:
        return None

def lag():
    for b in ("broker-1", "broker-2", "broker-3"):
        try:
            out = subprocess.run(["docker", "exec", b, "kafka-consumer-groups", "--bootstrap-server", f"{b}:29092",
                                  "--describe", "--group", "connect-es-sink-logs"], capture_output=True, text=True, timeout=15).stdout
        except Exception:
            continue
        t = sum(int(l.split()[5]) for l in out.splitlines() if len(l.split()) >= 6 and l.split()[1] == "kafka-logs" and l.split()[5].isdigit())
        if "kafka-logs" in out:
            return t
    return None

print(f"{'time':8} {'generated':>10} {'acked':>10} {'in_elastic':>11} {'acked-in_es':>12} {'sink_lag':>9}")
while True:
    g, a = ledger(); e = es(); l = lag() if scenario == "kafka" else "-"
    print(f"{time.strftime('%H:%M:%S'):8} {g:>10} {a:>10} {str(e):>11} {('n/a' if e is None else a - e):>12} {str(l):>9}", flush=True)
    time.sleep(5)
