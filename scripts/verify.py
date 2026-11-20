#!/usr/bin/env python3
"""
verify.py - compare the sources' ground-truth LEDGER with what really arrived (Elasticsearch, and the Kafka topic).

  acked          : the source got a 2xx from its agent ("someone owns this data now")
  acked_lost     : acked but NEVER arrived in Elasticsearch  -> SILENT DATA LOSS (nobody will resend it)
  unacked        : the source never got a 2xx (client knows; with retries this should be ~0)
  duplicates     : documents in ES minus distinct event_ids
  kafka_total/distinct : records in the Kafka topic vs distinct event_ids (where duplicates are created)
stdlib only; talks to ES on localhost:9200 and to Kafka through `docker exec broker-N`.
"""
import argparse, csv, datetime as dt, glob, json, os, sqlite3, statistics, subprocess, sys, time
import urllib.error, urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def http(method, url, body=None, timeout=60):
    req = urllib.request.Request(url, method=method, data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read() or b"{}")


def es_count(es, index):
    try:
        http("POST", f"{es}/{index}/_refresh")
        return http("GET", f"{es}/{index}/_count")["count"]
    except Exception:
        return None


def extract_event_id(src):
    """event_id location depends on the path: kafka sink => top level (JSON body), otel raw mapping => attributes/body."""
    for k in ("event_id", "Attributes.event_id", "attributes.event_id"):
        if k in src:
            return src[k]
    for top in ("Attributes", "attributes", "Body", "body"):
        v = src.get(top)
        if isinstance(v, dict) and "event_id" in v:
            return v["event_id"]
        if isinstance(v, str) and "event_id" in v:
            try:
                return json.loads(v)["event_id"]
            except Exception:
                pass
    for k, v in src.items():                       # flattened keys like "Attributes.event_id" / "attributes"."event_id"
        if k.endswith("event_id") and isinstance(v, str):
            return v
    return None


def parse_ts(s):
    try:
        return dt.datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
    except Exception:
        return None


def es_scan(es, index):
    docs = []
    try:
        r = http("POST", f"{es}/{index}/_search?scroll=2m", {"size": 5000, "query": {"match_all": {}}, "version": True})
    except urllib.error.HTTPError:
        return docs
    sid = r.get("_scroll_id")
    while r["hits"]["hits"]:
        for h in r["hits"]["hits"]:
            docs.append((h["_id"], h["_source"], h.get("_version", 1)))
        r = http("POST", f"{es}/_search/scroll", {"scroll": "2m", "scroll_id": sid})
        sid = r.get("_scroll_id", sid)
    try:
        http("DELETE", f"{es}/_search/scroll", {"scroll_id": sid})
    except Exception:
        pass
    return docs


def sh(cmd, timeout=120):
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout).stdout
    except Exception:
        return ""


def kafka_lag():
    """Sum of lag of the sink connector's consumer group, or None if Kafka can't be queried."""
    for b in ("broker-1", "broker-2", "broker-3"):
        out = sh(["docker", "exec", b, "kafka-consumer-groups", "--bootstrap-server", f"{b}:29092",
                  "--describe", "--group", "connect-es-sink-logs"], 30)
        lag, seen = 0, False
        for line in out.splitlines():
            p = line.split()
            if len(p) >= 6 and p[1] == "kafka-logs":
                seen = True
                lag += int(p[5]) if p[5].isdigit() else 0
        if seen:
            return lag
    return None


def kafka_topic_stats():
    for b in ("broker-1", "broker-2", "broker-3"):
        out = sh(["docker", "exec", b, "kafka-console-consumer", "--bootstrap-server", f"{b}:29092", "--topic", "kafka-logs",
                  "--from-beginning", "--timeout-ms", "10000"], 600)
        if out:
            ids = []
            for line in out.splitlines():
                try:
                    v = json.loads(line)
                    if isinstance(v, str):
                        v = json.loads(v)
                    ids.append(v["event_id"])
                except Exception:
                    pass
            return len(ids), len(set(ids)), set(ids)
    return None, None, set()


def gateway_conflicts():
    """Deliveries Elastic rejected as version conflicts (a second write of an existing _id); the Gateway logs each one."""
    ids = sh(["docker", "ps", "-aq", "--filter", "name=gateway"]).split()
    total = 0
    for c in ids:
        try:
            out = subprocess.run(["docker", "logs", c], capture_output=True, text=True, timeout=120)
            total += (out.stdout + out.stderr).count("version_conflict_engine_exception")
        except Exception:
            pass
    return total


def pct(vals, p):
    if not vals:
        return None
    vals = sorted(vals)
    return vals[min(len(vals) - 1, int(len(vals) * p))]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--variant", default="?")
    ap.add_argument("--scenario", default="otel")
    ap.add_argument("--label", default=None)
    ap.add_argument("--es", default="http://localhost:9200")
    ap.add_argument("--ledger", default=os.path.join(ROOT, "data", "ledger"))
    ap.add_argument("--wait", type=int, default=0, help="max seconds to wait for the pipeline to quiesce")
    ap.add_argument("--stable", type=int, default=30, help="seconds the ES count must stay unchanged")
    a = ap.parse_args()
    index = f"{a.scenario}-logs"
    label = a.label or f"{a.variant}__adhoc"

    # ---- ledger
    ledger = {}
    per_src = {}
    for f in sorted(glob.glob(os.path.join(a.ledger, "ledger-*.db"))):
        src = os.path.basename(f)[7:-3]
        db = sqlite3.connect(f"file:{f}?mode=ro", uri=True)
        rows = db.execute("SELECT event_id, gen_ts_ms, acked, attempts, ack_ts_ms FROM events").fetchall()
        db.close()
        per_src[src] = rows
        for eid, g, ack, att, ackts in rows:
            ledger[eid] = (src, g, ack, att, ackts)
    if not ledger:
        sys.exit(f"no ledger found in {a.ledger} (run ./load.sh first)")
    n_acked = sum(1 for v in ledger.values() if v[2])
    print(f"ledger: {len(ledger)} events generated, {n_acked} acked by the agents")

    # ---- quiesce: wait until ES stops growing (and the sink has no lag)
    if a.wait:
        deadline, last, since = time.time() + a.wait, -1, time.time()
        print(f"waiting for pipeline to quiesce (stable {a.stable}s, max {a.wait}s)...", flush=True)
        while time.time() < deadline:
            c = es_count(a.es, index)
            lag = kafka_lag() if a.scenario == "kafka" else 0
            # unknown sink lag (None) or lag > 0 means the sink is still working
            if c != last or lag != 0:
                last, since = c, time.time()
            print(f"  es_docs={c} distinct_acked_expected={n_acked}" + (f" sink_lag={lag}" if a.scenario == "kafka" else ""), flush=True)
            # still short of the acknowledged count: a stall (retry back-off, restart) may be only a pause, so wait much longer before calling it loss
            need = a.stable if c >= n_acked else max(a.stable, 90)
            if time.time() - since >= need:
                break
            time.sleep(5)

    # how often the SOURCES resent a batch (after a timeout or an error); each resend of a pending batch is an extra delivery attempt
    source_resends = sum(max(x[3] - 1, 0) for x in ledger.values())
    source_resent_events = sum(1 for x in ledger.values() if x[3] > 1)
    source_max_attempts = max((x[3] for x in ledger.values()), default=1)

    # ---- Elasticsearch
    docs = es_scan(a.es, index)
    seen, ingested, unknown, no_id = {}, {}, 0, 0
    overwritten = 0
    for _id, s, ver in docs:
        if a.scenario != "kafka":   # on the Kafka path _version is the Kafka offset (external versioning), not an overwrite count
            overwritten += max(int(ver) - 1, 0)
        eid = extract_event_id(s)
        if eid is None:
            no_id += 1
            continue
        seen[eid] = seen.get(eid, 0) + 1
        t = parse_ts(s.get("ingested_at", ""))
        if t is not None and (eid not in ingested or t < ingested[eid]):
            ingested[eid] = t
        if eid not in ledger:
            unknown += 1
    delivered = [e for e in ledger if e in seen]
    acked_ids = [e for e, v in ledger.items() if v[2]]
    acked_lost = [e for e in acked_ids if e not in seen]
    unacked = [e for e, v in ledger.items() if not v[2]]
    unacked_delivered = [e for e in unacked if e in seen]
    dup_docs = sum(c - 1 for c in seen.values())
    lat = [ingested[e] - ledger[e][1] / 1000 for e in delivered if e in ingested]
    # how long the SOURCES waited for their acknowledgement (event creation -> the Agent answered 2xx)
    ack_lat = [(x[4] - x[1]) / 1000 for x in ledger.values() if x[2] and x[4]]

    # ---- duplicate DELIVERIES to Elastic, counted even when a deterministic _id makes Elastic keep one copy
    rejected = gateway_conflicts() if a.scenario == "otel" else 0
    dup_deliveries = dup_docs + overwritten + rejected

    # ---- Kafka topic (where do duplicates / losses happen?)
    k_total = k_distinct = k_lost = None
    kids = set()
    if a.scenario == "kafka":
        k_total, k_distinct, kids = kafka_topic_stats()
        if k_total is not None:
            k_lost = sum(1 for e in acked_ids if e not in kids)

    # Kafka path: every duplicate record in the topic is written to Elastic again by the sink (collapsed by _id)
    topic_dups = (k_total - k_distinct) if (a.scenario == "kafka" and k_total is not None) else 0
    dup_deliveries = dup_docs + overwritten + rejected + topic_dups

    # ---- ATTRIBUTION: who is responsible for each event that never reached Elastic?
    #   source_gave_up : never acked (retry off / retry window expired)  -> the data SOURCE's responsibility
    #   agent_or_producer : acked, but never in the Kafka topic (kafka) / lost between agent and ES (otel)  -> the collector acked before the data was safe
    #   kafka_sink     : in the Kafka topic, but never reached Elastic -> the sink connector / Elastic
    lost_all = [e for e in ledger if e not in seen]
    attr_ = dict(source_gave_up=0, agent_or_producer=0, kafka_sink=0)
    lost_attempts = {"first_try": 0, "retried": 0}
    samples = {}
    for e in lost_all:
        src, g, ack, att = ledger[e][:4]
        if not ack:
            k = "source_gave_up"
        elif a.scenario == "kafka" and k_total is not None and e in kids:
            k = "kafka_sink"
        else:
            k = "agent_or_producer"
        attr_[k] += 1
        if ack:
            lost_attempts["retried" if att > 1 else "first_try"] += 1
        samples.setdefault(k, []).append((src, e))
    res = dict(label=label, variant=a.variant, scenario=a.scenario, generated=len(ledger), acked=len(acked_ids),
               delivered_distinct=len(delivered), es_docs=len(docs), acked_lost=len(acked_lost),
               acked_lost_pct=round(100 * len(acked_lost) / max(len(acked_ids), 1), 3), unacked=len(unacked),
               unacked_but_delivered=len(unacked_delivered), duplicates=dup_docs,
               duplicate_pct=round(100 * dup_docs / max(len(delivered), 1), 3),
               source_resends=source_resends, source_resent_events=source_resent_events, source_max_attempts=source_max_attempts,
               duplicates_overwritten=overwritten, duplicates_rejected=rejected, duplicates_in_topic=topic_dups, duplicate_deliveries=dup_deliveries,
               duplicate_deliveries_pct=round(100 * dup_deliveries / max(len(acked_ids), 1), 3), unknown_docs=unknown, docs_without_event_id=no_id,
               kafka_topic_total=k_total, kafka_topic_distinct=k_distinct, acked_missing_in_kafka=k_lost,
               latency_p50_s=pct(lat, .5), latency_p95_s=pct(lat, .95), latency_p99_s=pct(lat, .99), latency_max_s=max(lat) if lat else None,
               ack_latency_p50_s=pct(ack_lat, .5), ack_latency_p95_s=pct(ack_lat, .95), ack_latency_p99_s=pct(ack_lat, .99), ack_latency_max_s=max(ack_lat) if ack_lat else None,
               per_source={s: dict(generated=len(r), acked=sum(1 for x in r if x[2]),
                                   lost=sum(1 for x in r if x[2] and x[0] not in seen)) for s, r in per_src.items()},
               attribution=attr_, acked_lost_by_attempts=lost_attempts,
               finished=time.strftime("%Y-%m-%d %H:%M:%S"))

    f = lambda v: "n/a" if v is None else (f"{v:.1f}s" if isinstance(v, float) else str(v))
    print("\n" + "=" * 66)
    print(f" RESULT  {label}")
    print("=" * 66)
    print(f" events generated by the sources   {res['generated']:>9}")
    print(f" acknowledged to the sources       {res['acked']:>9}")
    print(f" delivered to Elastic (distinct)   {res['delivered_distinct']:>9}")
    print(f" ACKED BUT LOST (silent loss)      {res['acked_lost']:>9}   ({res['acked_lost_pct']}% of acked)")
    print(f" never acked (source knows)        {res['unacked']:>9}   (of which delivered anyway: {res['unacked_but_delivered']})")
    print(f" DUPLICATE documents in Elastic    {res['duplicates']:>9}   ({res['duplicate_pct']}%)")
    print(f" duplicate DELIVERIES to Elastic   {res['duplicate_deliveries']:>9}   ({res['duplicate_deliveries_pct']}% of acked): stored twice {dup_docs}, overwritten {overwritten}, rejected as conflicts {rejected}, duplicated in the Kafka topic {topic_dups}")
    print(f" sources resent {source_resent_events} events {source_resends} times in total (max {source_max_attempts} attempts for one event)")
    if a.scenario == "kafka":
        print(f" Kafka topic records / distinct    {f(k_total):>9} / {f(k_distinct)}   (acked missing in Kafka: {f(k_lost)})")
    print(f" delivery latency p50/p95/p99/max  {f(res['latency_p50_s'])} / {f(res['latency_p95_s'])} / {f(res['latency_p99_s'])} / {f(res['latency_max_s'])}")
    print(f" source ack latency p50/p95/p99/max {f(res['ack_latency_p50_s'])} / {f(res['ack_latency_p95_s'])} / {f(res['ack_latency_p99_s'])} / {f(res['ack_latency_max_s'])}")
    if no_id:
        print(f" WARNING: {no_id} ES docs had no recognisable event_id (check extract_event_id)")
    print(f" WHO LOST IT?  source gave up (never acked): {attr_['source_gave_up']}   agent/producer acked-before-safe: {attr_['agent_or_producer']}   kafka->ES sink: {attr_['kafka_sink']}")
    print(f"   acked-lost events by attempts: first try={lost_attempts['first_try']} retried-by-source={lost_attempts['retried']}")
    for s, d in res["per_source"].items():
        print(f"   {s}: generated={d['generated']} acked={d['acked']} lost={d['lost']}")
    verdict = "NO acked data lost" if not acked_lost else f"DATA LOSS: {len(acked_lost)} acked events never arrived"
    verdict += ", no duplicates" if not dup_docs else f", {dup_docs} duplicates"
    print(f"\n VERDICT: {verdict}\n")

    os.makedirs(os.path.join(ROOT, "results"), exist_ok=True)
    with open(os.path.join(ROOT, "results", f"{label}.json"), "w") as fh:
        json.dump(res, fh, indent=2)


if __name__ == "__main__":
    main()
