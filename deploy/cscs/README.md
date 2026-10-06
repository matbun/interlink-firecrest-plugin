# Virtual node for CSCS (Alps) through FirecREST

A separate interLink virtual node, next to any others in the cluster: the
upstream interLink helm chart (virtual kubelet, interLink API, official slurm
plugin) plus the bridge, added with kustomize because the chart has no
sidecar hook. Nothing here uses SSH: the only connection is HTTPS from the
bridge to `api.cscs.ch` and `auth.cscs.ch`.

```
Deployment cscs-firecrest-node, namespace interlink-cscs
  init: install-shims    copies sbatch/squeue/scancel/sinfo shims into bridge-shims
  vk, interlink          from the chart
  plugin                 official image; its Slurm commands are the shims
  firecrest-bridge       answers the shims through FirecREST, mirrors the job dir
emptyDirs: jobs (the JobRoot path), bridge-socket, bridge-shims
```

## Before you start

- A FirecREST client from the CSCS Developer Portal (client id and secret). Jobs
  run as the user who owns it.
- A project with compute time on the target system, for `Account`.
- The bridge image `registry.cern.ch/interlink/firecrest-bridge:0.1.0`
  (published by the `0.1.0` release).
- Outbound HTTPS from the cluster to `api.cscs.ch` and `auth.cscs.ch`.

## Deploy

1. Fill every `<...>` in `values.yaml`, `FirecrestConfig.yaml` and the `jobs`
   mount path in `kustomization.yaml`. The job root path must be identical in
   all four places: `DataRootFolder`, `JobRoot` and both `jobs` mounts.
2. Put the client secret in a file named `client-secret` here (gitignored):

   ```bash
   printf '%s' '<client secret>' > client-secret
   ```

3. Render the chart and apply:

   ```bash
   helm repo add interlink https://interlink-hq.github.io/interlink-helm-chart/
   helm template cscs interlink/interlink --version 0.6.2-pre4 \
     -n interlink-cscs --no-hooks -f values.yaml > rendered.yaml
   kubectl create namespace interlink-cscs
   kubectl apply -k .
   ```

   `--no-hooks` leaves out the chart's helm hooks: applied with kubectl, its
   pre-delete cleanup Job would run immediately and delete the node.

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

## Known limits

- With `ContainerRuntime: enroot` the slurm plugin (`0.6.3-pre2`) mounts the
  pod's env file over `/etc/environment`, which enroot does not load: pod env
  vars do not reach the container.
- No Services into the job: there is no network tunnel to Alps.
- Logs reach `kubectl logs` with up to `SyncInterval` (5 s) delay.
