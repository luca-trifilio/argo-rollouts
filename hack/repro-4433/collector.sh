#!/usr/bin/env bash
# Writes live.json about once a second: weights, targets, rollout, per-second HTTP codes, key events, for a live dashboard (the author's argo-4433-watch pane reads it).
# usage: ./collector.sh > /tmp/rollouts-4433-lbc/collector.log 2>&1 &
cd "${REPRO_WORK_DIR:-/tmp/rollouts-4433-lbc}" || exit 1
export KUBECONFIG=$PWD/kubeconfig AWS_ENDPOINT_URL=http://localhost:4567 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1 AWS_PAGER=""; unset AWS_PROFILE
[ -f traffic.last.json ] || echo '{"active":false,"ok":0,"err":0,"other":0,"perSec":[]}' > traffic.last.json
tg() { aws elbv2 describe-target-health --target-group-arn "$1" --query 'TargetHealthDescriptions[].TargetHealth.State' --output json | jq -c '{healthy:(map(select(.=="healthy"))|length), initial:(map(select(.=="initial"))|length), other:(map(select(.!="healthy" and .!="initial"))|length)}'; }
ctrl_events() { perl -MTime::Local -ne '
  next unless /time="(\d+)-(\d+)-(\d+)T(\d+):(\d+):(\d+)([+-])(\d\d):(\d\d)"/;
  ($Y,$M,$D,$h,$m,$s,$sg,$oh,$om)=($1,$2,$3,$4,$5,$6,$7,$8,$9);
  $e=timegm($s,$m,$h,$D,$M-1,$Y) - ($sg eq "+"?1:-1)*($oh*3600+$om*60); @g=gmtime($e); $t=sprintf("%02d:%02d:%02d",@g[2,1,0]);
  if(/Switched selector for service \x27([^\x27]+)\x27 from \x27([^\x27]+)\x27 to \x27([^\x27]+)\x27/){print "$t\tcontroller\tSwitched selector of $1: $2 -> $3\n"}
  elsif(/New weights:.*Canary:WeightDestination\{Weight:(\d+).*Stable:WeightDestination\{Weight:(\d+)/){print "$t\tcontroller\tSetWeight canary=$1 stable=$2\n"}
  elsif(/(not verified: \d+\/\d+ endpoints registered)/){print "$t\tcontroller\t$1\n"}
  elsif(/ verified: (\d+) endpoints registered/){print "$t\tcontroller\tverified: $1 endpoints registered\n"}' controller.log | tail -40; }
# elb-shim log: what the real LBC asked the ALB to do, and when the ALB put each target in rotation
alb_events() { docker logs --since 15m elb-shim 2>&1 | S=${STABLE_TG: -16} perl -ne '
  next unless /^(\d\d:\d\d:\d\d)\.\d+ (REGISTER|DEREGISTER|ADMITTED) (?:\(held, initial\) )?(\S+).*tg=(\S+)/; $n=($3=~tr/,//)+1; $g=($4 eq $ENV{S})?"stable":"canary";
  if($2 eq "REGISTER"){print "$1\talb\t$g TG: LBC registered $n target(s), state initial\n"}
  elsif($2 eq "DEREGISTER"){print "$1\talb\t$g TG: LBC deregistered $n target(s)\n"}
  else{print "$1\talb\t$g TG: target passed health checks, in rotation\n"}'; }
while true; do
  source env.sh
  W=$(aws elbv2 describe-rules --listener-arn "$LISTENER_ARN" --query 'Rules[?Priority==`"1"`]|[0].Actions[0].ForwardConfig.TargetGroups[].[TargetGroupArn,Weight]' --output json | jq -c --arg s "$STABLE_TG" '{stable:(map(select(.[0]==$s)|.[1])[0] // 0), canary:(map(select(.[0]!=$s)|.[1])[0] // 0)}')
  RO=$(kubectl -n repro-4433 get rollout transaction -o json | jq -c '{phase:.status.phase, step:(.status.currentStepIndex // 0), steps:(.spec.strategy.canary.steps|length), stableRS:(.status.stableRS // ""), replicas:.spec.replicas, image:(.spec.template.spec.containers[0].image|split(":")[1])}')
  T=$(docker logs loadgen 2>/dev/null | awk '{ if($2=="200"){a[$1]++;TA++} else if($2 ~ /^5/){b[$1]++;TB++} else {o[$1]++;TO++}; seen[$1]=1 } END{ for(k in seen) printf "%s\t%d\t%d\t%d\n",k,a[k],b[k],o[k]; printf "~TOTAL\t%d\t%d\t%d\n",TA,TB,TO }' | sort)
  if [ -n "$(sed '/^~TOTAL/d' <<<"$T")" ]; then
    jq -Rn '[inputs|split("\t")] as $r | ($r|map(select(.[0]=="~TOTAL"))[0]) as $t | ($r|map(select(.[0]!="~TOTAL"))|.[0:-1]|.[-90:]|map({t:.[0],ok:(.[1]|tonumber),err:(.[2]|tonumber),other:(.[3]|tonumber)})) as $p | {active:true, ok:($t[1]|tonumber), err:($t[2]|tonumber), other:($t[3]|tonumber), perSec:$p}' <<<"$T" > traffic.new.json && mv traffic.new.json traffic.last.json
  else jq -c '.active=false' traffic.last.json > traffic.new.json && mv traffic.new.json traffic.last.json; fi
  EV=$( { alb_events; ctrl_events; } | sort -s -k1,1 | awk -F'\t' '{if($3==pm && $2==ps){n++; line[cnt]=sprintf("%s\t%s\t%s  x%d",pt,$2,$3,n)} else {cnt++; n=1; pt=$1; ps=$2; pm=$3; line[cnt]=sprintf("%s\t%s\t%s",$1,$2,$3)}} END{for(i=1;i<=cnt;i++)print line[i]}' | tail -16 | jq -Rn '[inputs|split("\t")|{t:.[0],src:.[1],msg:.[2]}]')
  RUN=false; [ -f run.lock ] && RUN=true
  SHIM=false; [ "$(docker inspect -f '{{.State.Running}}' elb-shim 2>/dev/null)" = true ] && SHIM=true
  jq -n --arg t "$(date -u +%T)" --argjson w "$W" --argjson ro "$RO" --argjson s "$(tg "$STABLE_TG")" --argjson c "$(tg "$CANARY_TG")" --slurpfile tr traffic.last.json --argjson ev "$EV" --argjson run "$RUN" --argjson shim "$SHIM" \
    '{t:$t, weights:$w, rollout:$ro, tg:{stable:$s,canary:$c}, traffic:$tr[0], events:$ev, running:$run, admission:$shim}' > live.new.json && mv live.new.json live.json
  sleep 0.3
done
