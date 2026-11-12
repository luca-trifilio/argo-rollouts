#!/usr/bin/env bash
# usage: ./run-update.sh <tag> <image-tag>
# Runs one canary rollout to argoproj/rollouts-demo:<image-tag> under load, then asserts on the promotion window.
# Exit code: the one of assert-promotion.sh (0 = no 5xx after the stable selector switch).
HERE=$(cd "$(dirname "$0")" && pwd); WORK=${REPRO_WORK_DIR:-/tmp/rollouts-4433-lbc}
cd "$WORK" || exit 2; export KUBECONFIG=$PWD/kubeconfig
TAG=$1; IMG=$2
[ -n "$TAG" ] && [ -n "$IMG" ] || { echo "usage: $0 <tag> <image-tag>" >&2; exit 2; }
OLD=$(kubectl -n repro-4433 get rollout transaction -o jsonpath='{.status.stableRS}')
[ -n "$OLD" ] || { echo "[$TAG] cannot read the Rollout's stableRS" >&2; exit 2; }
# Start only from a settled Rollout: Healthy, and every target healthy in both target groups.
source env.sh
healthy() { AWS_ENDPOINT_URL=http://localhost:4567 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1 AWS_PAGER="" \
  aws elbv2 describe-target-health --target-group-arn "$1" --query 'TargetHealthDescriptions[].TargetHealth.State' --output text | tr '\t' '\n' | sort | uniq -c | tr -s ' ' | xargs; }
PHASE=$(kubectl -n repro-4433 get rollout transaction -o jsonpath='{.status.phase}'); S=$(healthy "$STABLE_TG"); C=$(healthy "$CANARY_TG")
[ "$PHASE" = Healthy ] && [ "$S" = "8 healthy" ] && [ "$C" = "8 healthy" ] ||
  { echo "[$TAG] not settled (rollout $PHASE, stable TG: ${S:-none}, canary TG: ${C:-none}); wait for Healthy and 8 healthy targets in each" >&2; exit 2; }
"$HERE/start-load.sh" 8 >/dev/null; sleep 3
"$HERE/sampler.sh" > "sampler-$TAG.log" 2>&1 & OV=$!
START=$(date -u +%Y-%m-%dT%H:%M:%SZ)
echo "[$TAG] old stable=$OLD trigger at $START"
kubectl -n repro-4433 patch rollout transaction --type=json -p "[{\"op\":\"replace\",\"path\":\"/spec/template/spec/containers/0/image\",\"value\":\"argoproj/rollouts-demo:$IMG\"}]" >/dev/null
HEALTHY=""
for i in $(seq 1 90); do
  s=$(kubectl -n repro-4433 get rollout transaction -o jsonpath='{.status.phase} {.status.stableRS}')
  if [ "$s" != "Healthy $OLD" ] && [[ "$s" == Healthy* ]]; then HEALTHY=$(date -u +%Y-%m-%dT%H:%M:%SZ); break; fi
  sleep 2
done
[ -n "$HEALTHY" ] && echo "[$TAG] rollout Healthy at $HEALTHY" || echo "[$TAG] rollout not Healthy after $i polls"
sleep 20; kill $OV 2>/dev/null
docker logs loadgen > "traffic-$TAG.log" 2>&1; docker rm -f loadgen >/dev/null 2>&1
END=$(date -u +%Y-%m-%dT%H:%M:%SZ)
printf 'START=%s\nHEALTHY=%s\nEND=%s\nOLD_STABLE=%s\nIMAGE=%s\n' "$START" "$HEALTHY" "$END" "$OLD" "$IMG" > "times-$TAG.env"
kubectl -n kube-system logs deploy/aws-load-balancer-controller --since-time="$START" > "lbc-$TAG.log" 2>&1; docker logs elb-shim --since "$START" > "shim-$TAG.log" 2>&1
echo "[$TAG] finished at $(date -u +%T); status codes:"; awk '{print $2}' "traffic-$TAG.log" | sort | uniq -c
echo "[$TAG] logs in $WORK: sampler-$TAG.log traffic-$TAG.log lbc-$TAG.log shim-$TAG.log times-$TAG.env"
"$HERE/assert-promotion.sh" "$TAG"
