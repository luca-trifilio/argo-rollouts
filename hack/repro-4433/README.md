# Promotion 503 repro (argoproj/argo-rollouts#4433)

A local, scripted reproduction of the ALB 503s at canary promotion: right after the **selector switch** of
the stable service, the **final weight shift** sends all traffic to a stable target group whose new targets are still
`initial`, and the ALB answers 503 for about 10 s. `run.sh` runs one canary rollout under constant load and exits
non-zero if any 5xx is served after the stable selector switch, so the same command proves the bug on master and
proves a fix.

Not run in CI. Everything is local: k3d, the real AWS Load Balancer Controller (LBC), and the Floci AWS emulator.

## How it works

- **Floci** (`floci start`, `http://localhost:4566`) emulates EC2 and ELBv2 with a real HTTP data plane: the ALB
  listener forwards to the registered IP targets by the weights in its rule.
- **aws-load-balancer-controller**, the real one, installed by Helm in k3d. It owns the Ingress, creates the listener,
  rule, target groups and TargetGroupBindings, and registers and deregisters pod IPs.
- **`elb-shim.py`**, a proxy in front of Floci's ELBv2 API on port 4567. Floci routes to a target the moment it is
  registered; a real ALB sends it nothing until it passes its initial health checks. The shim acknowledges
  `RegisterTargets`, reports the target as `initial`, and puts it in Floci's rotation only after it passes the target
  group's own health checks (`HealthyThresholdCount` checks, one per `HealthCheckIntervalSeconds`; `ADMIT_CHECKS=n`
  overrides the count). Without the shim every rollout returns only 200s.

**Target admission time is the one modelled part**; the call ordering of the LBC and of Argo Rollouts is real. The
assert output says so; say so too when quoting numbers.

The app (`app.yaml.in`): a Rollout of 8 replicas of `argoproj/rollouts-demo`, ALB traffic routing (no ping-pong),
steps `setWeight: 10`, `pause: 20s`, health check every 5 s, healthy after 2 checks, pod readiness gates on.

## Prerequisites

- Docker, running.
- `k3d`, `kubectl`, `helm`, `aws` (CLI v2), `jq`, `perl`.
- The `floci` CLI.
- Go as pinned by `go.mod` (`go 1.26.10`). With a newer local Go, prefix builds with `GOTOOLCHAIN=go1.26.10`.

## Work dir

The scripts stay here and run from anywhere. Everything they generate goes to the work dir, `/tmp/rollouts-4433-lbc`
(override with `REPRO_WORK_DIR`): `kubeconfig`, `vpc.env`, `app.yaml`, `env.sh`, the controller binary and every log.
Nothing is written to the repo. The `argo-4433-watch` pane reads `live.json` from that fixed path.

## Setup

```bash
floci start
hack/repro-4433/setup.sh
```

`setup.sh` is idempotent. It creates the k3d cluster `argo-rollouts` if missing (without touching your default
kubeconfig) and writes an isolated `kubeconfig` to the work dir, installs the Argo Rollouts CRDs from
`manifests/crds`, routes the pod CIDR into Floci, creates the VPC, starts the shim, installs the LBC, pre-creates the
ALB, applies `app.yaml` and writes `env.sh`. Rerun it after any Floci restart (Floci loses its state). Rerunning it
re-applies `app.yaml`, which resets the image to `blue` and so may start a rollout.

Build the controller and start it through the shim (port 4567), scoped by instance id so it ignores other Rollouts:

```bash
GOTOOLCHAIN=go1.26.10 go build -o /tmp/rollouts-4433-lbc/rollouts-controller ./cmd/rollouts-controller
cd /tmp/rollouts-4433-lbc
export KUBECONFIG=$PWD/kubeconfig AWS_ENDPOINT_URL=http://localhost:4567 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=us-east-1
./rollouts-controller --instance-id repro4433 --aws-verify-target-group --leader-elect=false \
  --healthzPort 18080 --metricsPort 18090 > controller.log 2>&1 &
```

The assert reads `controller.log` in the work dir (override with `CONTROLLER_LOG`). Only one controller may run with
instance id `repro4433`: two of them fight over the Rollout. To run a second one, give it another `--instance-id`
and relabel the Rollout (`argo-rollouts.argoproj.io/controller-instance-id`).

Wait until the Rollout is Healthy and both target groups show 8 healthy targets:

```bash
source env.sh
kubectl -n repro-4433 get rollout transaction -o jsonpath='{.status.phase}{"\n"}'
AWS_ENDPOINT_URL=http://localhost:4567 aws elbv2 describe-target-health --target-group-arn $STABLE_TG \
  --query 'TargetHealthDescriptions[].TargetHealth.State' --output text   # same for $CANARY_TG
```

## Run and assert

```bash
hack/repro-4433/run.sh; echo "exit $?"
```

`run.sh` picks an image the Rollout is not on and calls `run-update.sh <tag> <image>` with tag `run-HHMMSS`. That
refuses to start (exit 2) unless the Rollout is Healthy with 8 healthy targets in each target group, then starts the load (8 workers through Floci's ALB), patches the image, waits for the Rollout to be Healthy, keeps the
load on for 20 s more, then calls `assert-promotion.sh <tag>`. Exit code: `0` no 5xx after the stable selector switch,
`1` 5xx after it, `2` incomplete run (no switch found, no traffic, never Healthy).

`assert-promotion.sh <tag>` can be rerun on any past run in the work dir. It:

- finds the stable selector switch of the run: the first `Switched selector for service 'transaction' from '<old>'
  to '<new>'` in `controller.log` at or after the trigger time, else the Rollout's `SwitchService` event with that
  message. The event is a weak fallback: client-go's event spam filter often drops it after the many
  `TargetGroupVerified` events of a run;
- counts the 5xx in `traffic-<tag>.log` from that second to the end of the run's traffic. The window runs past the
  moment the Rollout turns Healthy on purpose: on master the Rollout is Healthy in the same second as the switch, and
  the 503s come after it;
- reports the 5xx before the switch (the canary-start window at the first `setWeight`, out of scope here) separately,
  as information only.

Expected on master (a controller with the code of master `9962f9327`):

```
== promotion assert, run run-131146 ==
stable selector switch: 12:12:39 UTC, service transaction 8445bb54b8 -> cc554667b (from controller log /tmp/rollouts-4433-lbc/controller.log)
rollout Healthy: 12:12:39 UTC
promotion window: 12:12:39 .. 12:12:59 UTC (21s, switch to end of traffic), 3031 requests
  5xx after the switch: 1447 (47.7% of the window); 12:12:39 .. 12:12:49 UTC (11s), 89.6% of requests in that span
    12:12:39  15/151
    12:12:40  152/152
    ...
    12:12:48  144/144
    12:12:49  112/144
  other non-200 after the switch (timeouts etc., not counted): 0
before the switch (canary-start window; information only, not counted): 133 5xx of 7481 requests
note: target admission time is modelled by elb-shim.py (a new target gets traffic only after the target group's
      health checks pass, as on a real ALB). Call ordering of the AWS Load Balancer Controller and Argo Rollouts is real.
RESULT: FAIL (1447 5xx after the stable selector switch)
```

At the switch the controller logs `Service transaction (...) not verified: 0/8 endpoints registered`, then, in the same
second, `verified: 8 endpoints registered` and the final weight shift (`New weights: ... Canary ... Weight:0`), while
the stable target group still reports `8 initial`.

## Logs

Each run leaves, in the work dir: `sampler-<tag>.log`, `traffic-<tag>.log`, `lbc-<tag>.log` (LBC log since the
trigger), `shim-<tag>.log` (registrations, health checks and admissions) and `times-<tag>.env` (trigger, Healthy and
end times, UTC). `controller.log` is the controller's own log.

`traffic-<tag>.log` has one line per request, `HH:MM:SS <http code>` (UTC).

`sampler-<tag>.log` has one line about every 1.5 s:

```
12:12:40.142 | w S100/C0   | S-TG[8initial                ] C-TG[8healthy                ] | Healthy step2 stable=cc554667b           | 503=232  <<< 5xx
```

- time (UTC);
- `w S<stable>/C<canary>`: the weights in the ALB listener rule;
- `S-TG[...]`, `C-TG[...]`: the stable and canary target groups' targets by state, as the shim reports them
  (`initial` = registered but not yet in rotation);
- the Rollout's phase, step index and `stableRS`;
- HTTP codes seen in the last second, flagged `<<< 5xx` when any.

The fields come from separate API calls, so one line is not an atomic snapshot; use `traffic-<tag>.log` for counts.

To watch a run live, start `hack/repro-4433/collector.sh > /tmp/rollouts-4433-lbc/collector.log 2>&1 &` and open the
`argo-4433-watch` pane, which reads `live.json`.

## Pitfalls

- Floci's default VPC breaks the LBC (empty `VpcId`, panic in `computeAZIDsWithoutAZInfo`); `setup.sh` creates a VPC
  whose CIDR is the pod CIDR.
- The LBC cannot create the ALB on Floci (it sends `SubnetMappings`, gets an empty `VpcId` back and loops on
  `DuplicateLoadBalancerName`); `setup.sh` pre-creates it with the LBC's tracking tags so the LBC adopts it.
- After a Floci restart the cluster keeps TargetGroupBindings for target groups that no longer exist; `setup.sh`
  drops them.
- A namespace with an LBC-managed Ingress hangs in Terminating when the LBC cannot clean up: clear the Ingress
  finalizers with a merge patch.
- Trigger updates with a JSON patch on `/spec/template/spec/containers/0/image`. `kubectl set image rollout/...` fails
  ("no kind Rollout is registered") and the rollout silently never updates.
- Floci publishes only port 4566; the ALB listens on :80 inside its container, so the load generator runs in Floci's
  network namespace (`docker run --net container:floci`) with `Host: <LB_DNS>`.
- On macOS a bind mount of `/tmp` into a container does not show the files: `setup.sh` passes the shim inline. There
  is no `timeout` command, and zsh does not word-split `$VAR`: keep helpers in bash scripts.
- Target group verification checks registration, not health, and only for `targetType: ip` bindings.

## Cleanup

```bash
kill %1   # or the controller's pid
docker rm -f loadgen elb-shim elb-shim-port
k3d cluster delete argo-rollouts
floci stop
```
