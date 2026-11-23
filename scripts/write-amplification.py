"""Where a benchmark's checkpoint bytes went, relative to message bytes.

Run the bench with RUST_LOG=warn,sidra::db=debug, cut the log to the phase
of interest, and pass it (optionally skipping the first N checkpoints):
    uv run scripts/write-amplification.py /tmp/phase.log [skip]
"""
import sys,re
rows=[dict(re.findall(r'(\w+)=([\w.]+)',l))|{'t':l[:19]} for l in open(sys.argv[1]) if 'checkpoint published' in l]
skip=int(sys.argv[2]) if len(sys.argv)>2 else 0
rows=rows[skip:]
S=lambda k: sum(int(r.get(k,0)) for r in rows)
M=1<<20
msg=S('msg_bytes')
print(f"checkpoints {len(rows)}  from {rows[0]['t']} to {rows[-1]['t']}")
print(f"messages {S('msgs')}  {msg/M:.0f} MiB (uncompressed entry bytes)")
for k in ['run_bytes','run_bytes_d0','run_bytes_d1','run_bytes_d2','run_bytes_d3','relocated_run_bytes','internal_bytes','leaf_bytes']:
    print(f"{k:>20} {S(k)/M:8.0f} MiB  {S(k)/msg:5.2f}x msgs")
seg=S('run_bytes')+S('internal_bytes')+S('leaf_bytes')
print(f"{'segments total':>20} {seg/M:8.0f} MiB  {seg/msg:5.2f}x msgs")
lr=S('leaves_rewritten'); print(f"leaves rewritten {lr}, moved (no msgs) {S('leaves_moved')}, msgs per leaf rewrite {S('leaf_msgs')/max(1,lr-S('leaves_moved')):.1f}")
print(f"median msgs per checkpoint {sorted(int(r['msgs']) for r in rows)[len(rows)//2]}")
