"""Median and total checkpoint phase times from a benchmark log.

Run the bench with RUST_LOG=warn,sidra::db=debug and pass the log file:
    uv run scripts/checkpoint-phases.py /tmp/bench.log
"""
import sys,re,statistics as st
rows=[dict(re.findall(r'(\w+)=([\w.]+)',l)) for l in open(sys.argv[1]) if 'checkpoint published' in l]
rows=[r for r in rows if int(r['msgs'])>10000]
for k in ['lease_ms','pull_ms','plan_ms','upload_ms','publish_ms']:
    v=[int(r[k]) for r in rows]; print(f"{k:>11} median {st.median(v):6.0f}  total {sum(v):6d}")
print("checkpoints", len(rows))
