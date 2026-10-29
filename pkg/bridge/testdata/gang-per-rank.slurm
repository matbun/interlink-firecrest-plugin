#!/bin/bash
#SBATCH --job-name=g1
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=1
#SBATCH --output=/scratch/u/interlink/jobs/default-0b6f3c1e-8f0a-4d7e-9c2b-5a1d3e4f6a70/job.out
#SBATCH --cpus-per-task=1

set -uo pipefail

# interLink gang-scheduling job (interlink.eu/gang-name=g1)
# One sbatch --nodes=2 co-allocates the whole gang; each rank runs on one node.

# --- site CommandPrefix (batch level; PATH propagates to ranks via --export=ALL) ---
module load singularity

# --- publish head node for shadow/tunnel discovery (batch node == head node) ---
mkdir -p /scratch/u/interlink/jobs/.gang-heads && hostname -f > /scratch/u/interlink/jobs/.gang-heads/default-g1 || true

mapfile -t nodes < <(scontrol show hostnames "$SLURM_JOB_NODELIST")
head_node="${nodes[0]}"
head_ip=$(srun --nodes=1 --ntasks=1 -w "$head_node" hostname --ip-address | awk '{print $1}')
export MASTER_ADDR="$head_ip"
export MASTER_PORT=6379
export WORLD_SIZE=2
export RAY_ADDRESS="$head_ip:6379"
echo "gang " g1 " head_node=$head_node head_ip=$head_ip world_size=2"
export GANG_NNODES=2
export GANG_NODES="${nodes[*]}"
export GANG_HEAD_NODE="$head_node"
export GANG_GPUS_PER_NODE=0

declare -a RANK_PIDS=()

# rank 0 (head) pod 0b6f3c1e-8f0a-4d7e-9c2b-5a1d3e4f6a70
srun --overlap --nodes=1 --ntasks=1 -w "${nodes[0]}" \
     --export=ALL,RANK=0,WORLD_SIZE=2,MASTER_ADDR="$head_ip",MASTER_PORT=6379,RAY_ADDRESS="$head_ip:6379" \
     -o /scratch/u/interlink/jobs/default-0b6f3c1e-8f0a-4d7e-9c2b-5a1d3e4f6a70/job.out -e /scratch/u/interlink/jobs/default-0b6f3c1e-8f0a-4d7e-9c2b-5a1d3e4f6a70/job.err --open-mode=truncate \
  bash -c 'hostname -f > /scratch/u/interlink/jobs/default-0b6f3c1e-8f0a-4d7e-9c2b-5a1d3e4f6a70/compute-node || true
exec /scratch/u/interlink/jobs/default-0b6f3c1e-8f0a-4d7e-9c2b-5a1d3e4f6a70/job.sh' &
RANK_PIDS+=($!)

# readiness barrier: wait for the head coordinator before launching workers
head_ready=0
for i in $(seq 1 60); do
  # Generic readiness: the head (rank 0) opens MASTER_PORT (Ray GCS / torch
  # rendezvous). Probe the port from the head node ($head_ip/$MASTER_PORT
  # expand in the batch shell). The loop proceeds anyway after the timeout so
  # a workload that never opens the port is not blocked forever.
  if srun --overlap --nodes=1 --ntasks=1 -w "$head_node" bash -c "exec 3<>/dev/tcp/$head_ip/$MASTER_PORT" >/dev/null 2>&1; then head_ready=1; break; fi
  sleep 2
done
echo "gang head_ready=$head_ready"

# rank 1 (worker) pod 7c9e2a4b-1d3f-4e5a-8b6c-9d0e1f2a3b4c
srun --overlap --nodes=1 --ntasks=1 -w "${nodes[1]}" \
     --export=ALL,RANK=1,WORLD_SIZE=2,MASTER_ADDR="$head_ip",MASTER_PORT=6379,RAY_ADDRESS="$head_ip:6379" \
     -o /scratch/u/interlink/jobs/default-7c9e2a4b-1d3f-4e5a-8b6c-9d0e1f2a3b4c/job.out -e /scratch/u/interlink/jobs/default-7c9e2a4b-1d3f-4e5a-8b6c-9d0e1f2a3b4c/job.err --open-mode=truncate \
  bash -c 'hostname -f > /scratch/u/interlink/jobs/default-7c9e2a4b-1d3f-4e5a-8b6c-9d0e1f2a3b4c/compute-node || true
exec /scratch/u/interlink/jobs/default-7c9e2a4b-1d3f-4e5a-8b6c-9d0e1f2a3b4c/job.sh' &
RANK_PIDS+=($!)

# Wait for the head (rank 0 / driver) and capture its rc as the gang result.
DRIVER_RC=0
if [ "${#RANK_PIDS[@]}" -gt 0 ]; then
  wait "${RANK_PIDS[0]}"; DRIVER_RC=$?
fi
# Reap remaining ranks so the allocation frees.
for pid in "${RANK_PIDS[@]:1}"; do kill "$pid" 2>/dev/null || true; done
wait 2>/dev/null || true
echo "DRIVER_RC=$DRIVER_RC"
exit "$DRIVER_RC"
