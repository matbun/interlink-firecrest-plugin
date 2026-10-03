#!/usr/bin/env bash
# Local end-to-end test: the official interLink slurm plugin runs pods on
# FirecREST's demo cluster through the bridge, with nothing but the FirecREST
# API between them.
#
#   run.sh up     start FirecREST's demo stack (+ Apptainer) and plugin + bridge
#   run.sh test   run the scenarios against a running stack
#   run.sh down   stop plugin + bridge and the demo stack
#   run.sh        up, then test
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
F7T_REF=${F7T_REF:-b474395}
F7T_DIR=${F7T_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/firecrest-bridge/firecrest-v2}
PLUGIN=${PLUGIN:-http://127.0.0.1:${PLUGIN_PORT:-4000}}
PLUGIN_ENROOT=${PLUGIN_ENROOT:-http://127.0.0.1:${PLUGIN_ENROOT_PORT:-4001}}
JOBROOT=/home/fireuser/interlink/jobs
SLURM_CTR=f7t-slurm-1
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

compose() { docker compose -f "$HERE/compose.yaml" "$@"; }
f7t() { (cd "$F7T_DIR" && docker compose -p f7t -f docker-compose.yml -f "$HERE/firecrest-override.yaml" "$@"); }

up() {
	if [ ! -d "$F7T_DIR/.git" ]; then
		mkdir -p "$(dirname "$F7T_DIR")"
		git clone -q https://github.com/eth-cscs/firecrest-v2 "$F7T_DIR"
	fi
	git -C "$F7T_DIR" checkout -q "$F7T_REF"
	if ! docker image inspect slurm >/dev/null 2>&1; then
		(cd "$F7T_DIR" && docker compose -p f7t -f docker-compose.yml build slurm)
	fi
	docker build -q -t firecrest-bridge-test/slurm-apptainer:latest "$HERE/slurm-apptainer" >/dev/null
	# MinIO (S3 transfers) and PBS are not needed.
	f7t up -d --build firecrest slurm keycloak keycloak-create-user ssh-ca
	echo "waiting for FirecREST"
	for _ in $(seq 1 90); do
		code=$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:8000/status/systems || true)
		[ "$code" = 401 ] || [ "$code" = 200 ] && break
		sleep 2
	done
	compose up -d --force-recreate
	echo "waiting for the plugin"
	for _ in $(seq 1 30); do
		curl -sf -X GET -H 'Content-Type: application/json' -d '[]' "$PLUGIN/status" >/dev/null && return 0
		sleep 2
	done
	echo "plugin did not come up" >&2
	compose logs --tail 30 >&2
	return 1
}

down() {
	compose down -v
	[ -d "$F7T_DIR" ] && f7t down -v || true
}

# --- plugin API -------------------------------------------------------------

pod() { jq '.pod' "$HERE/pods/$1.json"; }
uid() { jq -r '.pod.metadata.uid' "$HERE/pods/$1.json"; }
dir() { echo "$JOBROOT/default-$(uid "$1")"; }

create() { curl -sf -X POST -H 'Content-Type: application/json' --data @"$HERE/pods/$1.json" "$PLUGIN/create"; }
delete() { pod "$1" | curl -s -o /dev/null -w '%{http_code}' -X POST -H 'Content-Type: application/json' --data @- "$PLUGIN/delete"; }
status() { pod "$1" | jq -s '.' | curl -sf -X GET -H 'Content-Type: application/json' --data @- "$PLUGIN/status"; }
logs() {
	local follow=${2:-false}
	jq -n --arg uid "$(uid "$1")" --arg name "$(jq -r '.pod.metadata.name' "$HERE/pods/$1.json")" --argjson follow "$follow" \
		'{Namespace:"default", PodUID:$uid, PodName:$name, ContainerName:"main", Opts:{Tail:0, Follow:$follow}}' |
		curl -s --max-time "${3:-30}" -X GET -H 'Content-Type: application/json' --data @- "$PLUGIN/getLogs" || true
}
# The plugin may answer from a status cache shared by all pods (10 s), so pick
# this pod's entry by UID instead of trusting the first one.
mine() { status "$1" | jq --arg uid "$(uid "$1")" '[.[] | select(.UID == $uid)][0] // {}'; }
state() { mine "$1" | jq -r '.containers[0].state // {} | keys[0] // ""'; }
exit_code() { mine "$1" | jq -r '.containers[0].state.terminated.exitCode'; }
node() { mine "$1" | jq -r '.nodeName // ""'; }

wait_state() { # pod state timeout
	for _ in $(seq 1 "$3"); do
		[ "$(state "$1" 2>/dev/null)" = "$2" ] && return 0
		sleep 1
	done
	return 1
}
remote_has() { docker exec -u fireuser "$SLURM_CTR" test -e "$1"; }
job_state() { docker exec -u fireuser "$SLURM_CTR" squeue --noheader -a --states=all -O StateCompact -j "$1" 2>/dev/null | tr -d ' ' || true; }

# --- assertions -------------------------------------------------------------

PASS=0
FAIL=0
check() { # description, then a command
	local what=$1
	shift
	if "$@"; then
		echo "  ok   $what"
		PASS=$((PASS + 1))
	else
		echo "  FAIL $what"
		FAIL=$((FAIL + 1))
	fi
}
# known reports a check that fails because of an upstream issue without failing the run.
known() {
	local what=$1
	shift
	if "$@"; then
		echo "  ok   $what"
		PASS=$((PASS + 1))
	else
		echo "  known $what"
	fi
}
contains() { grep -qF -- "$2" <<<"$1"; }
equals() { [ "$1" = "$2" ]; }

cleanup_pods() {
	for p in hello fail sleeper config ticker; do delete "$p" >/dev/null 2>&1 || true; done
	PLUGIN=$PLUGIN_ENROOT delete enroot >/dev/null 2>&1 || true
}

# Same flow through the plugin instance configured with the enroot runtime.
test_enroot() {
	local PLUGIN=$PLUGIN_ENROOT out
	scenario "enroot: the runtime Alps uses (enroot import/create/start in the job)"
	create enroot >/dev/null
	check "pod reaches terminated" wait_state enroot terminated 240
	check "exit code 0" equals "$(exit_code enroot)" 0
	out=$(logs enroot)
	check "logs carry the container output" contains "$out" "enroot container alpine 3.20"
	check "env file staged on the cluster" \
		docker exec -u fireuser "$SLURM_CTR" grep -q DEMO_VAR=through-enroot "$(dir enroot)/main_envfile.properties"
	# The plugin mounts the env file over /etc/environment, which enroot 4.2 does
	# not load (enroot start --env does); not a bridge issue.
	known "env var set in the container (slurm plugin enroot env handling)" contains "$out" "DEMO_VAR=through-enroot"
	check "imported image is in the job directory on the cluster" \
		docker exec -u fireuser "$SLURM_CTR" sh -c "ls $(dir enroot)/*.sqsh"
	check "imported image is not pulled back to the plugin" \
		compose exec -T bridge sh -c "! ls $(dir enroot)/*.sqsh 2>/dev/null"
	delete enroot >/dev/null
}

scenario() { echo; echo "== $*"; }

test_all() {
	cleanup_pods
	sleep 3

	scenario "ping: node resources come from sinfo through FirecREST"
	local ping
	ping=$(curl -sf -X GET -H 'Content-Type: application/json' -d '[]' "$PLUGIN/status")
	check "ping answers ok with resources ($ping)" contains "$ping" '"status":"ok"'

	scenario "hello: create, run, logs, delete"
	local out jid
	out=$(create hello)
	jid=$(jq -r '.PodJID' <<<"$out")
	check "sbatch through FirecREST returned job $jid" test -n "$jid" -a "$jid" != null
	check "job directory staged on the cluster" remote_has "$(dir hello)/job.slurm"
	check "pod reaches terminated" wait_state hello terminated 180
	check "exit code 0" equals "$(exit_code hello)" 0
	check "nodeName reported from the compute node" equals "$(node hello)" slurm
	out=$(logs hello)
	check "logs carry the container output" contains "$out" "hello from slurm alpine"
	check "logs carry the last line" contains "$out" "done"
	check "delete answers 200" equals "$(delete hello)" 200
	sleep 5
	check "remote job directory removed" bash -c "! docker exec -u fireuser $SLURM_CTR test -e $(dir hello)"

	scenario "fail: a non-zero exit code reaches the pod status"
	create fail >/dev/null
	check "pod reaches terminated" wait_state fail terminated 180
	check "exit code 3" equals "$(exit_code fail)" 3
	check "logs carry the output" contains "$(logs fail)" "about to fail"
	delete fail >/dev/null

	scenario "config: ConfigMap volume, env and emptyDir reach the container"
	create config >/dev/null
	check "pod reaches terminated" wait_state config terminated 180
	out=$(logs config)
	check "ConfigMap mounted" contains "$out" "greeting=hello-from-configmap"
	check "env var set" contains "$out" "DEMO_VAR=set-in-pod-spec"
	check "emptyDir writable" contains "$out" "scratch=scratch"
	check "exit code 0" equals "$(exit_code config)" 0
	delete config >/dev/null

	scenario "ticker: follow logs live, survive a bridge restart"
	create ticker >/dev/null
	check "pod reaches running" wait_state ticker running 180
	sleep 4
	compose restart bridge >/dev/null 2>&1
	out=$(logs ticker true 120)
	check "followed log streams to the end" contains "$out" "ticker done"
	check "followed log has every tick" contains "$out" "tick 12"
	check "pod reaches terminated after the restart" wait_state ticker terminated 60
	check "exit code 0" equals "$(exit_code ticker)" 0
	delete ticker >/dev/null

	scenario "sleeper: deleting a running pod cancels its job"
	out=$(create sleeper)
	jid=$(jq -r '.PodJID' <<<"$out")
	check "pod reaches running" wait_state sleeper running 180
	check "delete answers 200" equals "$(delete sleeper)" 200
	sleep 5
	check "Slurm job $jid cancelled" equals "$(job_state "$jid")" CA
	check "remote job directory removed" bash -c "! docker exec -u fireuser $SLURM_CTR test -e $(dir sleeper)"

	test_enroot

	echo
	echo "passed $PASS, failed $FAIL"
	[ "$FAIL" -eq 0 ]
}

case "${1:-all}" in
up) up ;;
test) test_all ;;
down) down ;;
all) up && test_all ;;
*)
	echo "usage: $0 [up|test|down]" >&2
	exit 2
	;;
esac
