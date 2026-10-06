#!/usr/bin/env bash
# End-to-end test in kind: a Kubernetes pod scheduled on an interLink virtual
# node runs on FirecREST's demo cluster through the official slurm plugin and
# the bridge. Needs the demo stack from test/integration/run.sh up.
#
#   run.sh up     kind cluster + interLink (helm chart) + bridge
#   run.sh test   run pod.yaml and check status, logs and cleanup
#   run.sh down   delete the kind cluster
#   run.sh        up, then test
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
CLUSTER=${CLUSTER:-firecrest-bridge}
CHART=${CHART:-$HERE/../../../interlink-helm-chart/interlink}
NODE=firecrest-node
F7T_NET=f7t_firecrest-internal-v2
SLURM_CTR=f7t-slurm-1
# Only the local bridge image is loaded; the node pulls the published ones
# (kind load fails on multi-arch images that docker holds one platform of).
BRIDGE_IMAGE=firecrest-bridge:dev
export KUBECONFIG=${KUBECONFIG_E2E:-$HERE/.kubeconfig}

up() {
	if ! kind get clusters | grep -qx "$CLUSTER"; then
		local config=$HERE/kind.yaml
		[ "$(stat -fc %T /sys/fs/cgroup)" = cgroup2fs ] || config=$HERE/kind-cgroupv1.yaml
		kind create cluster --name "$CLUSTER" --config "$config" --kubeconfig "$KUBECONFIG" --wait 120s
	fi
	docker network connect "$F7T_NET" "$CLUSTER-control-plane" 2>/dev/null || true
	kind load docker-image --name "$CLUSTER" "$BRIDGE_IMAGE"
	helm template firecrest "$CHART" -n interlink --no-hooks -f "$HERE/values.yaml" >"$HERE/rendered.yaml"
	kubectl create namespace interlink --dry-run=client -o yaml | kubectl apply -f -
	kubectl apply -k "$HERE"
	kubectl -n interlink rollout status deploy/$NODE-node --timeout=300s
	echo "waiting for node $NODE"
	wait_node
	approve_csrs
}

# The virtual kubelet asks for a serving certificate, which kubectl logs needs,
# on every start. Approve its pending requests; wait for one if none is issued.
approve_csrs() {
	local who="system:serviceaccount:interlink:$NODE"
	for _ in $(seq 1 30); do
		local pending issued
		pending=$(kubectl get csr -o go-template='{{range .items}}{{if and (eq .spec.username "'"$who"'") (not .status.conditions)}}{{.metadata.name}} {{end}}{{end}}')
		if [ -n "$pending" ]; then
			# shellcheck disable=SC2086
			kubectl certificate approve $pending
			return 0
		fi
		issued=$(kubectl get csr -o go-template='{{range .items}}{{if and (eq .spec.username "'"$who"'") .status.certificate}}x{{end}}{{end}}')
		[ -n "$issued" ] && return 0
		sleep 2
	done
	echo "no serving CSR from $NODE; kubectl logs may fail" >&2
}

wait_node() {
	for _ in $(seq 1 60); do
		[ "$(kubectl get node $NODE -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null)" = True ] && return 0
		sleep 2
	done
	return 1
}

PASS=0
FAIL=0
check() {
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
contains() { grep -qF -- "$2" <<<"$1"; }

wait_phase() { # pod phase timeout
	for _ in $(seq 1 "$3"); do
		[ "$(kubectl get pod "$1" -o jsonpath='{.status.phase}' 2>/dev/null)" = "$2" ] && return 0
		sleep 1
	done
	return 1
}

test_all() {
	kubectl delete -f "$HERE/pod.yaml" --ignore-not-found --wait=true >/dev/null
	approve_csrs >/dev/null
	echo
	echo "== node"
	check "virtual node $NODE is Ready" wait_node
	local cpu
	cpu=$(kubectl get node $NODE -o jsonpath='{.status.capacity.cpu}')
	check "node capacity reported (cpu=$cpu)" test -n "$cpu"

	echo
	echo "== pod on the FirecREST cluster"
	kubectl apply -f "$HERE/pod.yaml" >/dev/null
	check "pod reaches Running" wait_phase firecrest-hello Running 240
	check "pod reaches Succeeded" wait_phase firecrest-hello Succeeded 240
	local uid out
	uid=$(kubectl get pod firecrest-hello -o jsonpath='{.metadata.uid}')
	out=$(kubectl logs firecrest-hello 2>&1 || true)
	check "kubectl logs: container output" contains "$out" "hello from slurm, alpine"
	check "kubectl logs: ConfigMap content" contains "$out" "configmap: hello-from-a-kubernetes-configmap"
	check "kubectl logs: last line" contains "$out" "finished"
	check "exit code 0" bash -c "[ \"\$(kubectl get pod firecrest-hello -o jsonpath='{.status.containerStatuses[0].state.terminated.exitCode}')\" = 0 ]"
	check "job directory exists on the cluster" docker exec -u fireuser "$SLURM_CTR" test -d "/home/fireuser/interlink/jobs/default-$uid"

	kubectl delete -f "$HERE/pod.yaml" --wait=true >/dev/null
	sleep 6
	check "job directory removed from the cluster after kubectl delete" bash -c "! docker exec -u fireuser $SLURM_CTR test -e /home/fireuser/interlink/jobs/default-$uid"

	echo
	echo "passed $PASS, failed $FAIL"
	echo "kubectl logs output:"
	sed 's/^/    /' <<<"$out"
	[ "$FAIL" -eq 0 ]
}

case "${1:-all}" in
up) up ;;
test) test_all ;;
down) kind delete cluster --name "$CLUSTER" ;;
all)
	# Not "up && test_all": errexit does not apply inside a function on the left of &&.
	up
	test_all
	;;
*)
	echo "usage: $0 [up|test|down]" >&2
	exit 2
	;;
esac
