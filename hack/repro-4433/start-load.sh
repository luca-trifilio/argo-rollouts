#!/usr/bin/env bash
# usage: ./start-load.sh [workers]   -> container "loadgen"; output via: docker logs -f loadgen
# Each response is logged as "HH:MM:SS <http code>" (UTC, one line per request).
cd "${REPRO_WORK_DIR:-/tmp/rollouts-4433-lbc}" || exit 1; source env.sh; N=${1:-8}
docker rm -f loadgen >/dev/null 2>&1
docker run -d --name loadgen --net container:floci --entrypoint sh alpine/curl -c "
for i in \$(seq $N); do
  ( while :; do c=\$(curl -s -o /dev/null -m 3 -w '%{http_code}' -H 'Host: $LB_DNS' http://127.0.0.1:80/); echo \"\$(date -u +%T) \$c\"; sleep 0.05; done ) &
done; wait" >/dev/null && echo "load started ($N workers). stop: docker rm -f loadgen"
