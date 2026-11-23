"""Self and inclusive time per function from a samply profile.

    samply record --save-only --unstable-presymbolicate -o /tmp/p.json.gz -- <cmd>
    uv run scripts/profile-summary.py /tmp/p.json.gz [thread-name-substring] [top]
"""
import bisect
import collections
import gzip
import json
import re
import sys

path = sys.argv[1]
want = sys.argv[2] if len(sys.argv) > 2 else ""
top = int(sys.argv[3]) if len(sys.argv) > 3 else 40
prof = json.load(gzip.open(path))
syms = json.load(open(re.sub(r"\.json\.gz$", ".json.syms.json", path)))
strings = syms["string_table"]
# Per library: sorted symbol start addresses and names.
libsyms = {}
for d in syms["data"]:
    entries = sorted((s["rva"], strings[s["symbol"]]) for s in d["symbol_table"])
    libsyms[d["debug_name"]] = ([a for a, _ in entries], [n for _, n in entries])
libs = prof["libs"]


def name_of(thread, frame):
    ft, ft_func = thread["frameTable"], thread["funcTable"]
    func = ft["func"][frame]
    res = ft_func["resource"][func]
    addr = ft["address"][frame]
    if res is not None and res >= 0 and addr is not None and addr >= 0:
        lib = libs[thread["resourceTable"]["lib"][res]]
        table = libsyms.get(lib["debugName"])
        if table:
            i = bisect.bisect_right(table[0], addr) - 1
            if i >= 0:
                return table[1][i]
    return thread["stringArray"][ft_func["name"][func]] if "stringArray" in thread else str(addr)


def short(n):
    n = re.sub(r"::h[0-9a-f]{16}$", "", n)
    return n[:110]


self_t, incl = collections.Counter(), collections.Counter()
total = 0
for th in prof["threads"]:
    if want not in th.get("name", ""):
        continue
    st = th["stackTable"]
    names = {}
    for stack in th["samples"]["stack"]:
        if stack is None:
            continue
        total += 1
        seen = set()
        leaf = True
        s = stack
        while s is not None:
            f = st["frame"][s]
            n = names.get(f) or names.setdefault(f, short(name_of(th, f)))
            if leaf:
                self_t[n] += 1
                leaf = False
            if n not in seen:
                incl[n] += 1
                seen.add(n)
            s = st["prefix"][s]
print(f"{total} samples")
print("== self")
for n, c in self_t.most_common(top):
    print(f"{100*c/total:5.1f}%  {n}")
print("== inclusive")
for n, c in incl.most_common(top):
    print(f"{100*c/total:5.1f}%  {n}")
