# Virtual node for CSCS (Alps) through FirecREST

A separate interLink virtual node, next to any others in the cluster, installed
with `helm install` from the upstream interLink chart (0.6.2-pre5 or later): the
chart's virtual kubelet, interLink API and official slurm plugin, with the
bridge added through the chart's `extraContainers` and `extraInitContainers`.
Nothing here uses SSH: the only connection is HTTPS from the bridge to
`api.cscs.ch` and `auth.cscs.ch`.

```mermaid
flowchart TB
  subgraph pod["Pod cscs-firecrest-node"]
    direction LR
    init["install-shims<br/>(extraInitContainers)"] -- shims --> chart["vk, interlink, plugin<br/>(from the chart)"]
    chart -- "Slurm commands,<br/>job directory" --> bridge["firecrest-bridge<br/>(extraContainers)"]
  end
  subgraph cscs["CSCS"]
    direction LR
    firecrest["FirecREST<br/>api.cscs.ch"] --> alps["Slurm on Alps"]
  end
  pod -- "HTTPS, from the bridge" --> cscs
```

## Before you start

- A FirecREST client from the CSCS Developer Portal (client id and secret). Jobs
  run as the user who owns it.
- A project with compute time on the target system, for `FIRECREST_ACCOUNT`.
- Outbound HTTPS from the cluster to `api.cscs.ch` and `auth.cscs.ch`.
- Kyverno, for `sizing-policy.yaml` (optional, see "Run a pod there").

## Deploy

1. Fill every `<...>` in `values.yaml`. The job root path appears four times
   (`DataRootFolder`, `FIRECREST_JOB_ROOT` and the two `jobs` mounts) and must
   be identical in all of them.
2. Create the namespace and the Secret with the bridge's settings, the only
   object the chart does not create. Jobs are charged to `FIRECREST_ACCOUNT`.

   ```bash
   kubectl create namespace interlink-cscs
   kubectl -n interlink-cscs create secret generic firecrest-env \
     --from-literal=FIRECREST_URL=https://api.cscs.ch/hpc/firecrest/v2 \
     --from-literal=FIRECREST_TOKEN_URL=https://auth.cscs.ch/auth/realms/firecrest-clients/protocol/openid-connect/token \
     --from-literal=FIRECREST_SYSTEM='<system, e.g. daint>' \
     --from-literal=FIRECREST_ACCOUNT='<project>' \
     --from-literal=FIRECREST_CLIENT_ID='<client id>' \
     --from-literal=FIRECREST_CLIENT_SECRET='<client secret>'
   ```

3. Install:

   ```bash
   helm install cscs -n interlink-cscs -f values.yaml \
     https://github.com/interlink-hq/interlink-helm-chart/releases/download/interlink-0.6.2-pre5/interlink-0.6.2-pre5.tgz
   ```

   If helm stops at "no cached repo found" for one of your configured
   repositories, run `helm repo update` or prefix the command with
   `HELM_REPOSITORY_CONFIG=/dev/null`.

4. Approve the virtual kubelet's serving certificate, which `kubectl logs`
   needs (it asks again on every restart):

   ```bash
   kubectl get csr | grep cscs-firecrest
   kubectl certificate approve <csr name>
   ```

## Run a pod there

```yaml
spec:
  nodeSelector:
    kubernetes.io/hostname: cscs-firecrest
  tolerations:
    - key: virtual-node.interlink/no-schedule
      operator: Exists
```

The virtual node advertises zero CPU and memory, so it adds nothing to
anything that scales with cluster capacity, and the scheduler places a pod
there only if its CPU and memory requests are 0. The slurm plugin sizes the
Slurm job from the limits: without a memory limit it submits `--mem=1` (1 MB).
`sizing-policy.yaml` does both for every pod that selects the node: a request
becomes the limit when the container sets none, and requests become 0.

```bash
kubectl -n <namespace> apply -f sizing-policy.yaml
```

Without Kyverno, write the pod's resources that way yourself
(`requests: {cpu: "0", memory: "0"}` and the size in `limits`).

## Known limits

- With `ContainerRuntime: enroot` the slurm plugin (`0.6.3-pre2`) mounts the
  pod's env file over `/etc/environment`, which enroot does not load: pod env
  vars do not reach the container.
- No Services into the job: there is no network tunnel to Alps.
- Logs reach `kubectl logs` with up to 5 s delay (the bridge's sync interval).
