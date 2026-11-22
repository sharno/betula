#!/usr/bin/env bash
# Model-checks the protocol with everystate (TLC-compatible, no Java):
#   cargo install everystate --locked
# Correct configs must pass; each Bug* config must report a violation.
set -euo pipefail
cd "$(dirname "$0")"

run() {
  everystate -workers auto -json -config "$1.cfg" Betula.tla | tail -1
}

status() {
  sed -E 's/.*"status":"([a-z]+)".*/\1/'
}

fail=0
for cfg in Betula TwoAppliers; do
  s=$(run "$cfg" | status)
  echo "$cfg: $s (want success)"
  [[ $s == success ]] || fail=1
done
for cfg in BugDiscovery BugCarryDown BugUnfenced; do
  s=$(run "$cfg" | status)
  echo "$cfg: $s (want invariant)"
  [[ $s == invariant ]] || fail=1
done
exit $fail
