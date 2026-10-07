#!/usr/bin/env bash
# Renders the interLink chart with every values file of this repo and checks
# the virtual node pod: the bridge and its shim installer are there, the plugin
# runs the shims, and the job root is the same path everywhere it is set. No
# cluster needed.
#
#   run.sh [values.yaml ...]   default: deploy/*/values.yaml and test/e2e/values.yaml
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
CHART_VERSION=${CHART_VERSION:-0.6.2-pre5}
CHART=${CHART:-https://github.com/interlink-hq/interlink-helm-chart/releases/download/interlink-$CHART_VERSION/interlink-$CHART_VERSION.tgz}

if [ $# -eq 0 ]; then
	set -- "$ROOT"/deploy/*/values.yaml "$ROOT/test/e2e/values.yaml"
fi

FAIL=0
check() { # description, then a command
	local what=$1
	shift
	if "$@"; then
		echo "  ok   $what"
	else
		echo "  FAIL $what"
		FAIL=1
	fi
}
equal() { [ "$1" = "$2" ]; }
contains() { grep -qx -- "$2" <<<"$1"; }

for values in "$@"; do
	echo "== ${values#"$ROOT"/}"
	if ! out=$(HELM_REPOSITORY_CONFIG=/dev/null helm template vn "$CHART" -n interlink -f "$values" 2>&1); then
		echo "  FAIL helm template: $out"
		FAIL=1
		continue
	fi
	deploy=$(yq 'select(.kind == "Deployment")' <<<"$out")
	config=$(yq 'select(.kind == "ConfigMap" and (.metadata.name | test("-plugin-config$"))) | .data."plugin.yaml"' <<<"$out")
	containers=$(yq '.spec.template.spec.containers[].name' <<<"$deploy")
	bridge='.spec.template.spec.containers[] | select(.name == "firecrest-bridge")'
	plugin='.spec.template.spec.containers[] | select(.name == "plugin")'

	check "one Deployment" equal "$(yq 'select(.kind == "Deployment") | .metadata.name' <<<"$out" | grep -c .)" 1
	check "init container install-shims" contains "$(yq '.spec.template.spec.initContainers[].name' <<<"$deploy")" install-shims
	for c in vk interlink plugin firecrest-bridge; do
		check "container $c" contains "$containers" "$c"
	done
	for cmd in sbatch squeue scancel sinfo; do
		key=$(tr '[:lower:]' '[:upper:]' <<<"${cmd:0:1}")${cmd:1}Path
		check "plugin $key is the bridge shim" equal "$(yq ".$key" <<<"$config")" "/opt/firecrest-bridge/$cmd"
	done

	root=$(yq '.DataRootFolder' <<<"$config")
	root=${root%/}
	check "FIRECREST_JOB_ROOT = DataRootFolder ($root)" \
		equal "$(yq "$bridge | .env[] | select(.name == \"FIRECREST_JOB_ROOT\") | .value" <<<"$deploy")" "$root"
	check "plugin mounts jobs at DataRootFolder" \
		equal "$(yq "$plugin | .volumeMounts[] | select(.name == \"jobs\") | .mountPath" <<<"$deploy")" "$root"
	check "bridge mounts jobs at DataRootFolder" \
		equal "$(yq "$bridge | .volumeMounts[] | select(.name == \"jobs\") | .mountPath" <<<"$deploy")" "$root"
	check "plugin and bridge share the socket directory" \
		equal "$(yq "$plugin | .volumeMounts[] | select(.name == \"bridge-socket\") | .mountPath" <<<"$deploy")" \
		"$(yq "$bridge | .volumeMounts[] | select(.name == \"bridge-socket\") | .mountPath" <<<"$deploy")"
done

[ "$FAIL" -eq 0 ] && echo "all values files render" || { echo "render checks failed"; exit 1; }
