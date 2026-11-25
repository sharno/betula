#!/usr/bin/env bash
# Model-checks the protocol with everystate (TLC-compatible, no Java):
#   cargo install everystate --locked
# Correct configs must pass; each Bug* and Gap* config must report a violation.
set -euo pipefail
cd "$(dirname "$0")"

# everystate exits non-zero on a violation; the JSON status says which.
run() {
  { everystate -workers auto -json -config "$1.cfg" Sidra.tla || true; } | tail -1
}

status() {
  sed -E 's/.*"status":"([a-z]+)".*/\1/'
}

fail=0
check() {
  s=$(run "$1" | status)
  echo "$1: $s (want $2)"
  [[ $s == "$2" ]] || fail=1
}

for cfg in Sidra Sidra3; do check "$cfg" ok; done
for cfg in GapStalledFence BugGcFences BugUnfenced BugPartial BugClock BugOverwrite; do
  check "$cfg" invariant
done
exit $fail
