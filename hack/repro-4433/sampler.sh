#!/usr/bin/env bash
# One line per sample: time | rule weights | stable TG targets by state | canary TG targets by state | rollout | HTTP codes (last 1s)
cd "${REPRO_WORK_DIR:-/tmp/rollouts-4433-lbc}" || exit 1; source env.sh
export KUBECONFIG=$PWD/kubeconfig AWS_ENDPOINT_URL=http://localhost:4567 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1 AWS_PAGER=""; unset AWS_PROFILE
ts() { perl -MTime::HiRes=gettimeofday -MPOSIX=strftime -e '($s,$u)=gettimeofday; printf "%s.%03d", strftime("%H:%M:%S",gmtime($s)), $u/1000'; }
n() { aws elbv2 describe-target-health --target-group-arn "$1" --query 'TargetHealthDescriptions[].TargetHealth.State' --output text | tr '\t' '\n' | sed '/^$/d' | sort | uniq -c | awk '{printf "%s%s ",$1,$2}'; }
while true; do
  t=$(ts)
  w=$(aws elbv2 describe-rules --listener-arn "$LISTENER_ARN" --query 'Rules[?Priority==`"1"`]|[0].Actions[0].ForwardConfig.TargetGroups[].[TargetGroupArn,Weight]' --output json | jq -r --arg s "$STABLE_TG" '[.[] | (if .[0]==$s then "S" else "C" end)+(.[1]|tostring)] | sort | reverse | join("/")')
  s=$(n "$STABLE_TG"); c=$(n "$CANARY_TG")
  r=$(kubectl -n repro-4433 get rollout transaction -o jsonpath='{.status.phase} step{.status.currentStepIndex} stable={.status.stableRS}')
  codes=$(docker logs --since 1s loadgen 2>&1 | awk '{c[$2]++} END{for(k in c) printf "%s=%d ",k,c[k]}')
  flag=""; [[ "$codes" =~ 5[0-9][0-9]= ]] && flag=" <<< 5xx"
  printf '%s | w %-9s | S-TG[%-24s] C-TG[%-24s] | %-40s | %s%s\n' "$t" "$w" "$s" "$c" "$r" "${codes:-no-traffic}" "$flag"
done
