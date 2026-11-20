#!/usr/bin/env python3
"""Build results/summary.md + summary.csv from results/*.json (latency = delivery latency, source event time -> ingested in Elastic)"""
import csv, glob, json, os
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
rows = [json.load(open(f)) for f in sorted(glob.glob(os.path.join(ROOT, "results", "*.json")))]
fmt = lambda v: "n/a" if v is None else f"{v:.1f}s"
out = ["| experiment | acked | delivered | **acked-but-lost** | duplicates | kafka topic total/distinct | p50 | p95 | p99 |", "|---|---:|---:|---:|---:|---:|---:|---:|---:|"]
for r in rows:
    kt = f"{r['kafka_topic_total']}/{r['kafka_topic_distinct']}" if r.get("kafka_topic_total") is not None else "-"
    out.append(f"| {r['label']} | {r['acked']} | {r['delivered_distinct']} | **{r['acked_lost']}** ({r['acked_lost_pct']}%) | {r['duplicates']} ({r['duplicate_pct']}%) | {kt} | {fmt(r.get('latency_p50_s'))} | {fmt(r.get('latency_p95_s'))} | {fmt(r.get('latency_p99_s'))} |")
open(os.path.join(ROOT, "results", "summary.md"), "w").write("\n".join(out) + "\n")
cols = ["label", "generated", "acked", "delivered_distinct", "acked_lost", "acked_lost_pct", "unacked", "duplicates", "duplicate_pct", "kafka_topic_total", "kafka_topic_distinct", "latency_p50_s", "latency_p95_s", "latency_p99_s", "latency_max_s"]
with open(os.path.join(ROOT, "results", "summary.csv"), "w", newline="") as fh:
    w = csv.writer(fh); w.writerow(cols); [w.writerow([r.get(c) for c in cols]) for r in rows]
print("\n".join(out))
