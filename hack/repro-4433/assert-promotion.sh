#!/usr/bin/env bash
# usage: ./assert-promotion.sh <tag>      (run-update.sh calls it at the end of every run)
#
# Asserts that the ALB served no 5xx in the promotion window of run <tag>:
#   - finds the stable service's selector switch in that run (controller log, else the Rollout's SwitchService event);
#   - counts the 5xx in traffic-<tag>.log from that second to the end of the run's traffic
#     (the rollout reaching Healthy, plus the 20s run-update.sh keeps the load on after it);
#   - reports the 5xx before the switch (the canary-start window, out of scope) separately, as information only.
# Exit code: 0 = no 5xx after the switch, 1 = 5xx after the switch, 2 = missing or incomplete run data.
#
# Env: REPRO_WORK_DIR (default /tmp/rollouts-4433-lbc), CONTROLLER_LOG (default $REPRO_WORK_DIR/controller.log).
WORK=${REPRO_WORK_DIR:-/tmp/rollouts-4433-lbc}
cd "$WORK" || exit 2; export KUBECONFIG=$PWD/kubeconfig
TAG=$1; SVC=transaction; CLOG=${CONTROLLER_LOG:-$WORK/controller.log}
[ -n "$TAG" ] || { echo "usage: $0 <tag>   (runs in $WORK: $(ls times-*.env 2>/dev/null | sed 's/^times-//;s/\.env$//' | tr '\n' ' '))" >&2; exit 2; }
[ -f "times-$TAG.env" ] && [ -s "traffic-$TAG.log" ] || { echo "assert: no times-$TAG.env or traffic-$TAG.log in $WORK" >&2; exit 2; }
# shellcheck source=/dev/null
source "times-$TAG.env"   # START, HEALTHY, END (UTC), written by run-update.sh

epoch() { perl -MTime::Local -e '$ARGV[0] =~ /^(\d+)-(\d+)-(\d+)T(\d+):(\d+):(\d+)Z$/ or exit 1; print timegm($6,$5,$4,$3,$2-1,$1)' "$1"; }
START_E=$(epoch "$START") || { echo "assert: bad START in times-$TAG.env" >&2; exit 2; }

# The stable selector switch: first "Switched selector for service '<svc>'" logged at or after the trigger.
# The controller logs it twice (the msg and the recorded Event); both carry the same time.
SWITCH=""; SRC=""
if [ -f "$CLOG" ]; then
  SWITCH=$(S="$SVC" T="$START_E" perl -MTime::Local -ne '
    next unless /^time="(\d+)-(\d+)-(\d+)T(\d+):(\d+):(\d+)(?:\.\d+)?(Z|([+-])(\d\d):(\d\d))"/;
    $e = timegm($6,$5,$4,$3,$2-1,$1); $e -= ($8 eq "+" ? 1 : -1) * ($9*3600 + $10*60) if $7 ne "Z";
    next unless $e >= $ENV{T} && /Switched selector for service \x27\Q$ENV{S}\E\x27 from \x27([^\x27]*)\x27 to \x27([^\x27]*)\x27/;
    print "$e $1 $2\n"; exit' "$CLOG")
  [ -n "$SWITCH" ] && SRC="controller log $CLOG"
fi
if [ -z "$SWITCH" ]; then
  SWITCH=$(kubectl -n repro-4433 get events --field-selector reason=SwitchService,involvedObject.name=transaction -o json 2>/dev/null | jq -r --argjson t "$START_E" \
      --arg re "^Switched selector for service '$SVC' from '(?<f>[^']*)' to '(?<to>[^']*)'" '
    [.items[] | {e: ((.lastTimestamp // .eventTime // .metadata.creationTimestamp) | sub("\\.[0-9]+"; "") | fromdateiso8601), m: .message}
     | select(.e >= $t) | . + {c: (.m | capture($re))}]
    | sort_by(.e) | first | select(.) | "\(.e) \(.c.f) \(.c.to)"')
  [ -n "$SWITCH" ] && SRC="Rollout event SwitchService"
fi
[ -n "$SWITCH" ] || { echo "assert: no selector switch of service '$SVC' at or after $START (looked in $CLOG and the Rollout events)" >&2; exit 2; }
read -r SWITCH_E FROM TO <<<"$SWITCH"

# Traffic lines are "HH:MM:SS code" in UTC; anchor them to the trigger's day (and roll over midnight).
SVC="$SVC" SRC="$SRC" SW="$SWITCH_E" FROM="$FROM" TO="$TO" T0="$START_E" HEALTHY="$HEALTHY" TAG="$TAG" perl -MPOSIX=strftime -e '
  $sw = $ENV{SW}; $t0 = $ENV{T0}; $day = $t0 - $t0 % 86400;
  sub hms { strftime("%H:%M:%S", gmtime($_[0])) }
  while (<STDIN>) {
    next unless /^(\d\d):(\d\d):(\d\d) (\d{3})/; $t = $day + $1*3600 + $2*60 + $3; $t += 86400 if $t < $t0 - 43200; $t -= 86400 if $t > $t0 + 43200;
    $x = substr($4, 0, 1) eq "5"; $o = !$x && $4 ne "200";
    $last = $t if !defined $last || $t > $last;
    if ($t < $sw) { $pre_n++; $pre_x++ if $x; $pre_o++ if $o; next }
    $n++; $per{$t}++; if ($x) { $xx++; $perx{$t}++; $f //= $t; $l = $t } $oth++ if $o;
  }
  if (!$n) { print STDERR "assert: no traffic logged after the selector switch\n"; exit 2 }
  printf "== promotion assert, run %s ==\n", $ENV{TAG};
  printf "stable selector switch: %s UTC, service %s %s -> %s (from %s)\n", hms($sw), $ENV{SVC}, $ENV{FROM}, $ENV{TO}, $ENV{SRC};
  printf "rollout Healthy: %s\n", $ENV{HEALTHY} ? substr($ENV{HEALTHY}, 11, 8) . " UTC" : "never (run timed out)";
  printf "promotion window: %s .. %s UTC (%ds, switch to end of traffic), %d requests\n", hms($sw), hms($last), $last - $sw + 1, $n;
  if ($xx) {
    $sn = 0; $sn += $per{$_} // 0 for $f .. $l;
    printf "  5xx after the switch: %d (%.1f%% of the window); %s .. %s UTC (%ds), %.1f%% of requests in that span\n",
      $xx, 100*$xx/$n, hms($f), hms($l), $l - $f + 1, 100*$xx/$sn;
    printf "    %s  %d/%d\n", hms($_), $perx{$_}, $per{$_} for sort { $a <=> $b } keys %perx;
  } else { print "  5xx after the switch: 0\n" }
  printf "  other non-200 after the switch (timeouts etc., not counted): %d\n", $oth // 0;
  printf "before the switch (canary-start window; information only, not counted): %d 5xx of %d requests\n", $pre_x // 0, $pre_n // 0;
  print "note: target admission time is modelled by elb-shim.py (a new target gets traffic only after the target group\x27s\n",
        "      health checks pass, as on a real ALB). Call ordering of the AWS Load Balancer Controller and Argo Rollouts is real.\n";
  if (!$ENV{HEALTHY}) { print "RESULT: INCOMPLETE (rollout never became Healthy)\n"; exit 2 }
  if ($xx) { printf "RESULT: FAIL (%d 5xx after the stable selector switch)\n", $xx; exit 1 }
  print "RESULT: PASS (no 5xx after the stable selector switch)\n"; exit 0
' < "traffic-$TAG.log"
