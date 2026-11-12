#!/usr/bin/env bash
# Picks an image tag the Rollout is not on, runs one full canary rollout under load and asserts on the promotion window.
# Exit code: 0 = no 5xx after the stable selector switch, 1 = 5xx after it, 2 = the run or its data is incomplete.
HERE=$(cd "$(dirname "$0")" && pwd); WORK=${REPRO_WORK_DIR:-/tmp/rollouts-4433-lbc}
cd "$WORK" || exit 2; export KUBECONFIG=$PWD/kubeconfig
[ -f run.lock ] && { echo "a run is active ($WORK/run.lock)"; exit 2; }
CUR=$(kubectl -n repro-4433 get rollout transaction -o jsonpath='{.spec.template.spec.containers[0].image}' | cut -d: -f2)
for c in yellow red green orange purple blue; do [ "$c" != "$CUR" ] && NEW=$c && break; done
date > run.lock; trap 'rm -f run.lock' EXIT
"$HERE/run-update.sh" "run-$(date +%H%M%S)" "$NEW"
