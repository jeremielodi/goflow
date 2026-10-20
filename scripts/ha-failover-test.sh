#!/bin/bash
# Failover drill for the Patroni cluster (deploy/ha): starts GoFlow process
# instances waiting on a user task, stops the current Postgres primary,
# waits for Patroni to promote a replica, then checks that every instance
# is still there, that their tasks complete, and that new instances run.
#
#   scripts/ha-failover-test.sh [GoFlow URL] [admin email] [admin password]
#
# Needs curl, docker and the containers of deploy/ha/docker-compose.ha.yaml
# (goflow-pg1..3). Creates real instances: not for a production database.
set -euo pipefail

URL="${1:-http://127.0.0.1:8080}"
EMAIL="${2:-admin@goflow.com}"
PASSWORD="${3:-admin123}"
COUNT="${COUNT:-10}"

json() { sed -nE "s/.*\"$1\" *: *\"([^\"]+)\".*/\1/p" | head -1; }
role() { docker exec "goflow-$1" curl -s localhost:8008/patroni 2>/dev/null | sed -nE 's/.*"role": *"([a-z_]+)".*/\1/p'; }
primary() { for n in pg1 pg2 pg3; do r="$(role $n || true)"; [ "$r" = "master" ] || [ "$r" = "primary" ] && { echo "$n"; return; }; done; }

TOKEN="$(curl -sf -X POST "$URL/auth/login" -H 'Content-Type: application/json' \
	-d "{\"email\":\"$EMAIL\",\"password\":\"$PASSWORD\"}" | json access_token)"
auth=(-H "Authorization: Bearer $TOKEN")

BPMN="failover-drill.$$.bpmn" # current directory: a Windows curl reads no /tmp path
cat > "$BPMN" <<'BPMN'
<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" id="d" targetNamespace="t">
  <bpmn:process id="FailoverDrill" isExecutable="true">
    <bpmn:startEvent id="s"/>
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="approve"/>
    <bpmn:userTask id="approve"/>
    <bpmn:sequenceFlow id="f2" sourceRef="approve" targetRef="e"/>
    <bpmn:endEvent id="e"/>
  </bpmn:process>
</bpmn:definitions>
BPMN
curl -sf "${auth[@]}" -F "resources=@$BPMN;filename=failover.bpmn" -F deployment-name=failover-drill \
	"$URL/engine-rest/deployment/create" >/dev/null
rm -f "$BPMN"

start() { curl -sf "${auth[@]}" -H 'Content-Type: application/json' -d '{"variables":{}}' \
	"$URL/engine-rest/v2/process-definitions/FailoverDrill/start" | json processInstanceId; }

echo "Starting $COUNT instances…"
ids=()
for _ in $(seq "$COUNT"); do ids+=("$(start)"); done

OLD="$(primary)"
echo "Primary: $OLD — stopping it"
t0=$(date +%s)
docker stop "goflow-$OLD" >/dev/null

NEW=""
until [ -n "$NEW" ] && [ "$NEW" != "$OLD" ]; do sleep 1; NEW="$(primary || true)"; done
t1=$(date +%s)
echo "New primary: $NEW, promoted in $((t1 - t0)) s"

# GoFlow reconnects through HAProxy: retry until a start succeeds.
until id="$(start 2>/dev/null)" && [ -n "$id" ]; do sleep 1; done
t2=$(date +%s)
echo "GoFlow writing again $((t2 - t0)) s after the primary stopped"
ids+=("$id")

complete_all() {
	local done=0
	for pid in "${ids[@]}"; do
		task="$(curl -sf "${auth[@]}" "$URL/tasks?processInstanceId=$pid" | json id)"
		[ -n "$task" ] && curl -sf "${auth[@]}" -H 'Content-Type: application/json' -d '{"variables":{}}' \
			-X POST "$URL/tasks/$task/complete" >/dev/null && done=$((done + 1))
	done
	echo "$done"
}
completed="$(complete_all)"
echo "Tasks completed after the failover: $completed / ${#ids[@]}"

sleep 3
lost=0
for pid in "${ids[@]}"; do
	status="$(curl -sf "${auth[@]}" "$URL/engine-rest/process-instance/$pid" | json status || true)"
	[ "$status" = "completed" ] || { echo "  instance $pid: ${status:-missing}"; lost=$((lost + 1)); }
done

echo "Restarting goflow-$OLD (rejoins as a replica)"
docker start "goflow-$OLD" >/dev/null

if [ "$lost" -eq 0 ] && [ "$completed" -eq "${#ids[@]}" ]; then
	echo "OK: no instance lost, all ${#ids[@]} completed"
else
	echo "FAILED: $lost instance(s) not completed"
	exit 1
fi
