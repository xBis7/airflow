from __future__ import annotations

import json
import statistics
import sys
from collections import defaultdict

PATH = sys.argv[1] if len(sys.argv) > 1 else "files/jaeger_loops.json"

with open(PATH) as f:
    payload = json.load(f)

traces = payload["data"]


def tags(span):
    return {t["key"]: t["value"] for t in span.get("tags", [])}


def parent_id(span):
    for ref in span.get("references", []):
        if ref["refType"] == "CHILD_OF":
            return ref["spanID"]
    return None


def loop_span(trace):
    for s in trace["spans"]:
        if s["operationName"] == "scheduler.scheduler_loop":
            return s
    return None


# --- 1. Collect non-idle loops -------------------------------------------------
non_idle = []
idle = 0
no_loop = 0
for tr in traces:
    ls = loop_span(tr)
    if ls is None:
        no_loop += 1
        continue
    t = tags(ls)
    if t.get("airflow.scheduler.loop_iteration.idle") is False:
        non_idle.append((ls["duration"], tr, ls))
    else:
        idle += 1

non_idle.sort(key=lambda x: x[0], reverse=True)

print(
    f"total traces={len(traces)}  non_idle_loops={len(non_idle)}  idle_loops={idle}  no_loop_span={no_loop}"
)
print()

# --- 2. Aggregate self-time per operation across ALL non-idle loops ------------
# self_time = duration - sum(direct children durations), clamped at 0.
# db.query_ms is set by the SQLAlchemy hook on the innermost active span (self-DB
# semantics). We track both self (as set on the span) and inclusive (subtree sum).
op_self = defaultdict(list)  # op -> list of self-times (us)
op_total = defaultdict(list)  # op -> list of total (wall) durations (us)
op_count = defaultdict(int)
op_db_self = defaultdict(list)  # op -> list of db self-times (ms)
op_db_incl = defaultdict(list)  # op -> list of db inclusive-times (ms)


def build_db_rollups(spans, children):
    span_db_self = {s["spanID"]: float(tags(s).get("db.query_ms", 0) or 0) for s in spans}
    span_db_incl: dict = {}

    def rollup(sid):
        if sid in span_db_incl:
            return span_db_incl[sid]
        total = span_db_self.get(sid, 0.0)
        for c in children.get(sid, []):
            total += rollup(c["spanID"])
        span_db_incl[sid] = total
        return total

    for s in spans:
        rollup(s["spanID"])
    return span_db_self, span_db_incl


for _dur, tr, _ls in non_idle:
    spans = tr["spans"]
    by_id = {s["spanID"]: s for s in spans}
    children = defaultdict(list)
    for s in spans:
        p = parent_id(s)
        if p is not None:
            children[p].append(s)
    span_db_self, span_db_incl = build_db_rollups(spans, children)
    for s in spans:
        child_dur = sum(c["duration"] for c in children.get(s["spanID"], []))
        self_t = max(0, s["duration"] - child_dur)
        op = s["operationName"]
        op_self[op].append(self_t)
        op_total[op].append(s["duration"])
        op_count[op] += 1
        op_db_self[op].append(span_db_self.get(s["spanID"], 0.0))
        op_db_incl[op].append(span_db_incl.get(s["spanID"], 0.0))


def pct(vals, p):
    if not vals:
        return 0
    vals = sorted(vals)
    k = int(round((p / 100) * (len(vals) - 1)))
    return vals[k]


rows = []
for op in op_self:
    selfs = op_self[op]
    tot_self = sum(selfs)
    tot_db_self = sum(op_db_self[op])
    tot_db_incl = sum(op_db_incl[op])
    rows.append(
        (
            tot_self,
            op,
            op_count[op],
            statistics.mean(selfs),
            pct(selfs, 50),
            pct(selfs, 95),
            max(selfs),
            tot_db_self,
            tot_db_incl,
        )
    )

rows.sort(reverse=True)

print("=== Aggregate SELF-time per operation across non-idle loops (microseconds) ===")
print(
    f"{'total_self_ms':>13}  {'count':>6}  {'mean_us':>9}  {'p50_us':>8}  {'p95_us':>9}  {'max_us':>9}  {'db_self_ms':>10}  {'db_incl_ms':>10}  operation"
)
for tot_self, op, cnt, mean, p50, p95, mx, db_self, db_incl in rows:
    print(
        f"{tot_self / 1000:>13.1f}  {cnt:>6}  {mean:>9.0f}  {p50:>8.0f}  {p95:>9.0f}  {mx:>9.0f}  {db_self:>10.1f}  {db_incl:>10.1f}  {op}"
    )

# --- 3. Top 5 longest non-idle loops: tree breakdown --------------------------
print()
print("=== Top 5 longest non-idle scheduler_loop iterations ===")
for rank, (dur, tr, ls) in enumerate(non_idle[:5], 1):
    spans = tr["spans"]
    children = defaultdict(list)
    for s in spans:
        p = parent_id(s)
        if p is not None:
            children[p].append(s)
    span_db_self, span_db_incl = build_db_rollups(spans, children)
    lt = tags(ls)
    print(
        f"\n--- #{rank}  loop duration={dur / 1000:.1f} ms  "
        f"queued_tis={lt.get('airflow.scheduler.num_queued_tis')} "
        f"finished_events={lt.get('airflow.scheduler.num_finished_events')} "
        f"loop_count={lt.get('airflow.scheduler.loop_count')} "
        f"(trace {tr['traceID'][:12]}) ---"
    )

    def walk(span, depth):
        t = tags(span)
        child_dur = sum(c["duration"] for c in children.get(span["spanID"], []))
        self_t = max(0, span["duration"] - child_dur)
        db_s = span_db_self.get(span["spanID"], 0.0)
        db_i = span_db_incl.get(span["spanID"], 0.0)
        # show interesting numeric attrs
        attrs = {
            k.split(".")[-1]: v
            for k, v in t.items()
            if k.startswith("airflow.") and isinstance(v, (int, float)) and not isinstance(v, bool)
        }
        attr_s = "  " + " ".join(f"{k}={v}" for k, v in attrs.items()) if attrs else ""
        print(
            f"    {'  ' * depth}{span['operationName']:<52} "
            f"total={span['duration'] / 1000:7.1f}ms self={self_t / 1000:6.1f}ms "
            f"db_self={db_s:5.1f}ms db_incl={db_i:5.1f}ms{attr_s}"
        )
        for c in sorted(children.get(span["spanID"], []), key=lambda x: x["startTime"]):
            walk(c, depth + 1)

    walk(ls, 0)
