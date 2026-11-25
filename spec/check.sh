#!/usr/bin/env bash
# Model-checks the protocol with everystate (TLC-compatible, no Java):
#   cargo install everystate --locked
# Correct configs must pass; each Bug* config must report a violation.
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
# The full configs need far more memory than a laptop has since clock
# readings became unconstrained; see README.md.
full=()
[[ ${FULL:-} == 1 ]] && full=(Sidra TwoAppliers)
for cfg in ${full[@]+"${full[@]}"}; do
  s=$(run "$cfg" | status)
  echo "$cfg: $s (want ok)"
  [[ $s == ok ]] || fail=1
done
s=$(run PartialCap | status)
echo "PartialCap: $s (want ok)"
[[ $s == ok ]] || fail=1
for cfg in BugDiscovery BugCarryDown BugUnfenced BugBound BugPartial; do
  s=$(run "$cfg" | status)
  echo "$cfg: $s (want invariant)"
  [[ $s == invariant ]] || fail=1
done
exit $fail
