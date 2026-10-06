# interLink FirecREST plugin

Run [interLink](https://github.com/interlink-hq/interLink) pods on an HPC
cluster that is reachable only through [FirecREST v2](https://eth-cscs.github.io/firecrest-v2/),
such as CSCS Alps: no SSH, no shared filesystem.

It does not reimplement the pod-to-Slurm translation. The official
[interLink slurm plugin](https://github.com/interlink-hq/interlink-slurm-plugin)
runs unchanged, and `firecrest-bridge` gives it what it expects from a login
node: the Slurm commands and a job directory that the job can see.

```
virtual kubelet -> interLink API -> slurm plugin (official image, unchanged)
                                        | runs sbatch / squeue / scancel / sinfo
                                        v
                                    shims --unix socket--> firecrest-bridge --HTTPS--> FirecREST -> Slurm
                                                              | mirrors the job directory
```

## How it works

- **Shims.** The bridge binary, invoked as `sbatch`, `squeue`, `scancel` or
  `sinfo`, forwards the call to the daemon. The daemon answers through
  FirecREST and prints what Slurm 24.11 prints for the same arguments, so the
  plugin parses it as usual.
- **Staging.** `sbatch <dir>/job.slurm` uploads the job directory the plugin
  wrote (scripts, env files, volume content, empty dirs) to the same absolute
  path on the cluster, then submits the script.
- **Pulling back.** Every `SyncInterval` the daemon pulls the files the plugin
  reads from running jobs: logs (only the new tail), container and probe
  status files, and `compute-node`. Anything else the job leaves in its
  directory, such as an imported enroot image, stays on the cluster. Files are
  written in place, so a followed log keeps streaming. A terminal state is
  reported only after a final pull, so the plugin finds the container exit
  codes. Pending jobs are not polled.
- **Cleanup.** When the plugin removes a job directory, the bridge removes the
  remote copy. Nothing outside `JobRoot` is ever removed, and nothing at all
  while `JobRoot` itself is missing.
- **Restarts.** On start the daemon picks up running jobs from the plugin's
  `JobID.jid` files.

## Status

Tested against FirecREST's own demo stack (FirecREST 2.6, Slurm 24.11, Keycloak)
with the official slurm plugin `0.6.3-pre2` and interLink `0.6.3-pre2`:

- `test/integration/run.sh`, 33 checks: create, status, `nodeName`, logs,
  followed logs across a bridge restart, exit codes, ConfigMap, env and
  emptyDir volumes, delete of finished and running jobs with remote cleanup,
  and the same flow with the enroot runtime (Apptainer otherwise).
- `test/e2e/run.sh`, 10 checks: a pod applied with `kubectl` to the virtual
  node in kind runs on the FirecREST cluster; `kubectl logs` and
  `kubectl delete` work.

Not yet run against CSCS.

CI (`.github/workflows/`) runs on every pull request and push to `main`:
`ci.yaml` runs golangci-lint, `go vet` and the unit tests, and builds the
bridge image for amd64 and arm64 without pushing it; `e2e.yaml` runs both
scripts above on a GitHub runner. Publishing a GitHub release tagged `X.Y.Z`
runs `release.yaml`, which pushes
`registry.cern.ch/interlink/firecrest-bridge:X.Y.Z` with the `HARBOR_USERNAME`
and `HARBOR_PASSWORD` secrets of the `harbor` environment.

## Try it locally

Needs docker (with compose), kind, helm, kubectl, jq and Go 1.26.

```bash
make test                     # unit tests
make image                    # firecrest-bridge:dev
test/integration/run.sh       # FirecREST demo stack + plugin + bridge, then the scenarios
CHART=/path/to/interlink-helm-chart/interlink test/e2e/run.sh   # kind + interLink + bridge
test/e2e/run.sh down; test/integration/run.sh down
```

`test/integration/run.sh` clones `eth-cscs/firecrest-v2` at a pinned commit
into `~/.cache/firecrest-bridge`, adds Apptainer and enroot to its demo Slurm
node, and leaves out MinIO, PBS and the SSH CA (not needed). The kind test
uses `kindest/node:v1.34.3`, the last image whose kubelet starts on cgroup v1
hosts, where it also switches the kubelet to the cgroupfs driver.

## Deploying

The bridge runs next to the plugin, in the same pod. `deploy/cscs/` is a
ready-to-fill virtual node for CSCS, and `test/e2e/` the same layout as tested
in kind, both on top of the interLink helm chart:

- an init container, `firecrest-bridge install-shims /opt/firecrest-bridge`,
  copies the binary and the four shims into a volume the plugin mounts;
- the `firecrest-bridge daemon` container;
- three shared volumes: the job root (plugin and bridge, same path), the
  socket directory, and the shims (read-only in the plugin).

Plugin configuration (`SlurmConfig.yaml`), the lines that differ:

```yaml
SbatchPath: /opt/firecrest-bridge/sbatch
SqueuePath: /opt/firecrest-bridge/squeue
ScancelPath: /opt/firecrest-bridge/scancel
SinfoPath: /opt/firecrest-bridge/sinfo
DataRootFolder: /capstor/scratch/cscs/<user>/interlink/jobs/   # = bridge JobRoot
ImagePrefix: "docker://"
ContainerRuntime: enroot      # Alps runs the enroot-based Container Engine
```

Bridge configuration (`FirecrestConfig.yaml`), for example at CSCS:

```yaml
FirecrestURL: https://api.cscs.ch/hpc/firecrest/v2
System: daint
TokenURL: https://auth.cscs.ch/auth/realms/firecrest-clients/protocol/openid-connect/token
ClientID: <client from the CSCS developer portal>
ClientSecretFile: /etc/firecrest/client-secret
Account: <project>
JobRoot: /capstor/scratch/cscs/<user>/interlink/jobs
```

| Key | Default | Meaning |
|---|---|---|
| `FirecrestURL`, `System` | | FirecREST v2 base URL and cluster name |
| `TokenURL`, `ClientID`, `ClientSecret` / `ClientSecretFile` | | OAuth2 client credentials; the token is refreshed before it expires |
| `APIKey` / `APIKeyFile` | | CSCS service-account key (`X-API-Key`), instead of client credentials |
| `Account` | | passed to every submission |
| `JobRoot` | | the plugin's `DataRootFolder`, same absolute path in the pod and on the cluster |
| `Socket` | `/var/run/firecrest-bridge/bridge.sock` | where the shims connect (`FIRECREST_BRIDGE_SOCKET` for the shims) |
| `SyncInterval` | `5s` | how often running jobs are pulled |
| `FinalSyncRounds` | `2` | extra pulls after a job ends |
| `PingTTL` | `30s` | cache of the liveness check behind `squeue --me` |
| `UploadParallelism` | `4` | concurrent uploads at submission |
| `MaxUploadSize` | 5 MiB | FirecREST's direct upload limit |
| `RequestTimeout` | `60s` | per FirecREST call |

These can also come from the environment (`FIRECREST_URL`,
`FIRECREST_SYSTEM`, `FIRECREST_TOKEN_URL`, `FIRECREST_CLIENT_ID`,
`FIRECREST_CLIENT_SECRET`, `FIRECREST_API_KEY`, `FIRECREST_ACCOUNT`,
`FIRECREST_JOB_ROOT`, `FIRECREST_BRIDGE_SOCKET`).

## Limitations

- **No network tunnels.** interLink's wstunnel, mesh and SSH shadow modes need
  either a public endpoint on the Kubernetes side or SSH to the cluster.
  Alps compute nodes are on private addresses behind NAT, so reaching a pod's
  ports needs a rendezvous both sides can dial out to. Not implemented.
- **Files above 5 MiB** cannot be staged at submission: FirecREST's S3 transfer
  path is not implemented. Scripts, env files and ConfigMaps are far below it.
- **Logs lag** by up to `SyncInterval`.
- **One identity.** Every job runs as the owner of the FirecREST client.
- The `slurm-job.vk.io/job-workdir` annotation must point below `JobRoot`.
- With `ContainerRuntime: enroot`, the slurm plugin (`0.6.3-pre2`) mounts the
  env file over `/etc/environment`, which enroot 4.2 does not load: pod env
  vars do not reach the container. A plugin issue, independent of the bridge.
- Only the `squeue`, `sinfo` and `scancel` invocations the plugin makes are
  emulated.
