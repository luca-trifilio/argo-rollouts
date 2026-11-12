#!/usr/bin/env bash
# Idempotent: builds the whole repro environment. Run it from anywhere; the scripts stay in hack/repro-4433/
# and every generated file (kubeconfig, vpc.env, app.yaml, env.sh, logs) goes to the work dir.
# Rerun after any Floci restart: Floci loses its state and its IP may change.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd); WORK=${REPRO_WORK_DIR:-/tmp/rollouts-4433-lbc}
mkdir -p "$WORK"; cd "$WORK"
CLUSTER=argo-rollouts; K3D_NET=k3d-$CLUSTER; NODE=k3d-$CLUSTER-server-0; POD_CIDR=10.42.0.0/16; NS=repro-4433; LBC_VERSION=3.6.2
export KUBECONFIG=$PWD/kubeconfig AWS_ENDPOINT_URL=http://localhost:4566 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1 AWS_PAGER=""; unset AWS_PROFILE

[ "$(docker inspect -f '{{.State.Running}}' floci 2>/dev/null)" = true ] || { echo "the floci container is not running: run 'floci start' first" >&2; exit 1; }

# 0. the k3d cluster, with an isolated kubeconfig (the default kubectl context may be a real cluster)
if ! k3d cluster list $CLUSTER >/dev/null 2>&1; then
  k3d cluster create $CLUSTER --wait --kubeconfig-update-default=false --kubeconfig-switch-context=false >/dev/null
  rm -f kubeconfig
fi
[ -f kubeconfig ] || k3d kubeconfig write $CLUSTER -o kubeconfig >/dev/null
kubectl apply --server-side -k "$HERE/../../manifests/crds" >/dev/null

# 1. network: Floci's ALB data plane (and the shim's health checks) must reach pod IPs
docker network connect $K3D_NET floci 2>/dev/null || true
NODE_IP=$(docker inspect $NODE --format "{{(index .NetworkSettings.Networks \"$K3D_NET\").IPAddress}}")
FLOCI_IP=$(docker inspect floci --format "{{(index .NetworkSettings.Networks \"$K3D_NET\").IPAddress}}")
docker run --rm --net container:floci --cap-add NET_ADMIN alpine ip route replace $POD_CIDR via $NODE_IP

# 2. a VPC whose CIDR is the pod CIDR. Floci's default VPC breaks the LBC (empty VpcId, AZ panic).
[ -f vpc.env ] && source vpc.env
if [ -z "${VPC:-}" ] || ! aws ec2 describe-vpcs --vpc-ids "$VPC" >/dev/null 2>&1; then
  VPC=$(aws ec2 create-vpc --cidr-block $POD_CIDR --query Vpc.VpcId --output text)
  SA=$(aws ec2 create-subnet --vpc-id $VPC --cidr-block 10.42.0.0/17 --availability-zone us-east-1a --query Subnet.SubnetId --output text)
  SB=$(aws ec2 create-subnet --vpc-id $VPC --cidr-block 10.42.128.0/17 --availability-zone us-east-1b --query Subnet.SubnetId --output text)
  SG=$(aws ec2 create-security-group --group-name repro-alb --description repro --vpc-id $VPC --query GroupId --output text)
  echo "VPC=$VPC SA=$SA SB=$SB SG=$SG" > vpc.env
fi

# 3. ALB admission shim, in Floci's network namespace (:4567), plus a host port for the controller and scripts.
#    The script is passed inline: bind mounts of /tmp do not show up in containers on macOS.
docker rm -f elb-shim elb-shim-port >/dev/null 2>&1 || true
docker run -d --name elb-shim --net container:floci -e PYTHONUNBUFFERED=1 -e ADMIT_CHECKS=${ADMIT_CHECKS:-0} python:3.12-alpine python -c "$(cat "$HERE/elb-shim.py")" >/dev/null
docker run -d --name elb-shim-port --network $K3D_NET -p 4567:4567 alpine/socat tcp-listen:4567,fork,reuseaddr tcp:$FLOCI_IP:4567 >/dev/null

# 4. the real aws-load-balancer-controller, talking to the shim
helm repo add eks https://aws.github.io/eks-charts >/dev/null 2>&1 || true
helm upgrade --install aws-load-balancer-controller eks/aws-load-balancer-controller -n kube-system --version $LBC_VERSION \
  --set clusterName=$CLUSTER --set region=us-east-1 --set vpcId=$VPC --set replicaCount=1 \
  --set enableShield=false --set enableWaf=false --set enableWafv2=false \
  --set env.AWS_ENDPOINT_URL=http://$FLOCI_IP:4567 --set env.AWS_ACCESS_KEY_ID=test --set env.AWS_SECRET_ACCESS_KEY=test --set env.AWS_REGION=us-east-1 >/dev/null
kubectl -n kube-system rollout status deploy/aws-load-balancer-controller --timeout=180s >/dev/null

# 5. pre-create the ALB with --subnets and the LBC's tags so the LBC adopts it. The LBC itself creates it with
#    SubnetMappings, for which Floci returns an empty VpcId; the LBC then never finds it again and loops on
#    DuplicateLoadBalancerName. The LBC finds its load balancer by these tags, not by name.
if [ "$(aws elbv2 describe-load-balancers --query "length(LoadBalancers[?VpcId=='$VPC'])" --output text)" = 0 ]; then
  aws elbv2 create-load-balancer --name k8s-repro-4433 --scheme internal --type application --ip-address-type ipv4 --subnets $SA $SB --security-groups $SG \
    --tags Key=elbv2.k8s.aws/cluster,Value=$CLUSTER Key=ingress.k8s.aws/resource,Value=LoadBalancer Key=ingress.k8s.aws/stack,Value=$NS/transaction >/dev/null
fi

# 6. after a Floci restart the cluster still has TargetGroupBindings for target groups Floci no longer has, and the
#    LBC fails with "targetgroupbindings ... already exists". Drop them (their finalizer can never be cleared).
for b in $(kubectl -n $NS get targetgroupbinding -o name 2>/dev/null); do
  aws elbv2 describe-target-groups --target-group-arns "$(kubectl -n $NS get "$b" -o jsonpath='{.spec.targetGroupARN}')" >/dev/null 2>&1 && continue
  kubectl -n $NS patch "$b" --type=merge -p '{"metadata":{"finalizers":null}}' >/dev/null 2>&1 || true
  kubectl -n $NS delete "$b" --ignore-not-found >/dev/null
  echo "dropped stale $b"
done

# 7. the app; the LBC builds listener, rule, target groups and TargetGroupBindings
sed -e "s/__SUBNETS__/$SA,$SB/" -e "s/__SG__/$SG/" "$HERE/app.yaml.in" > app.yaml
kubectl apply -f app.yaml >/dev/null
LB_ARN=$(aws elbv2 describe-load-balancers --query "LoadBalancers[?VpcId=='$VPC']|[0].LoadBalancerArn" --output text)
lbc_done() {
  local arns; arns=$(kubectl -n $NS get targetgroupbinding -o jsonpath='{.items[*].spec.targetGroupARN}' 2>/dev/null)
  [ "$(wc -w <<<"$arns")" -ge 2 ] && aws elbv2 describe-target-groups --target-group-arns $arns >/dev/null 2>&1 &&
    [ "$(aws elbv2 describe-listeners --load-balancer-arn "$LB_ARN" --query 'length(Listeners)' --output text 2>/dev/null)" -ge 1 ] 2>/dev/null
}
for _ in $(seq 90); do lbc_done && break; sleep 2; done
lbc_done || { echo "the LBC did not build the listener and target groups in 180s; see: kubectl -n kube-system logs deploy/aws-load-balancer-controller" >&2; exit 1; }
tgb() { kubectl -n $NS get targetgroupbinding -o json | jq -r --arg s "$1" '.items[]|select(.spec.serviceRef.name==$s)|.spec.targetGroupARN'; }
cat > env.sh <<ENV
export STABLE_TG=$(tgb transaction) CANARY_TG=$(tgb transaction-canary) LB_ARN=$LB_ARN LISTENER_ARN=$(aws elbv2 describe-listeners --load-balancer-arn $LB_ARN --query 'Listeners[0].ListenerArn' --output text) LB_DNS=$(aws elbv2 describe-load-balancers --load-balancer-arns $LB_ARN --query 'LoadBalancers[0].DNSName' --output text)
ENV
cat env.sh; echo "ok. Work dir: $WORK. Now build and start the rollouts controller through the shim (see hack/repro-4433/README.md)."
