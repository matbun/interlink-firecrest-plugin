#!/bin/bash
#SBATCH --job-name=g1
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=1
#SBATCH --output=/scratch/u/interlink/jobs/default-0b6f3c1e-8f0a-4d7e-9c2b-5a1d3e4f6a70/job.out
#SBATCH --gres=gpu:4
#SBATCH --cpus-per-task=1

set -uo pipefail

# interLink gang-scheduling job (interlink.eu/gang-name=g1)
# One sbatch --nodes=2 co-allocates the whole gang; each rank runs on one node.

# --- site CommandPrefix (batch level; PATH propagates to ranks via --export=ALL) ---
module load singularity

# --- site file (/etc/interlink/site.sh) ---
source /scratch/u/interlink/jobs/default-0b6f3c1e-8f0a-4d7e-9c2b-5a1d3e4f6a70/site.sh

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
export GANG_GPUS_PER_NODE=4

# --- site launcher 'torchrun': torchrun-launcher starts every rank ---
export SRUN_CPUS_PER_TASK="${SLURM_CPUS_PER_TASK:-}"
export GANG_CONTAINER='singularity exec --nv --no-home   /gpfs/images/itwinai.sif'
hostname -f > /scratch/u/interlink/jobs/default-0b6f3c1e-8f0a-4d7e-9c2b-5a1d3e4f6a70/compute-node || true
srun --overlap -N1 -n1 -w "${nodes[1]}" hostname -f > /scratch/u/interlink/jobs/default-7c9e2a4b-1d3f-4e5a-8b6c-9d0e1f2a3b4c/compute-node || true
export GANG_LOG_DIR=/scratch/u/interlink/jobs/default-0b6f3c1e-8f0a-4d7e-9c2b-5a1d3e4f6a70/ranks
mkdir -p "$GANG_LOG_DIR"
: > /scratch/u/interlink/jobs/default-0b6f3c1e-8f0a-4d7e-9c2b-5a1d3e4f6a70/ranks/rank-0.out
ln -sfn /scratch/u/interlink/jobs/default-0b6f3c1e-8f0a-4d7e-9c2b-5a1d3e4f6a70/ranks/rank-0.out /scratch/u/interlink/jobs/default-0b6f3c1e-8f0a-4d7e-9c2b-5a1d3e4f6a70/run-main.out
: > /scratch/u/interlink/jobs/default-0b6f3c1e-8f0a-4d7e-9c2b-5a1d3e4f6a70/ranks/rank-1.out
ln -sfn /scratch/u/interlink/jobs/default-0b6f3c1e-8f0a-4d7e-9c2b-5a1d3e4f6a70/ranks/rank-1.out /scratch/u/interlink/jobs/default-7c9e2a4b-1d3f-4e5a-8b6c-9d0e1f2a3b4c/run-main.out
( torchrun-launcher python train.py ) >>/scratch/u/interlink/jobs/default-0b6f3c1e-8f0a-4d7e-9c2b-5a1d3e4f6a70/ranks/rank-0.out 2>&1
LAUNCHER_RC=$?
echo "gang launcher 'torchrun' exited rc=$LAUNCHER_RC"
echo "$LAUNCHER_RC" > /scratch/u/interlink/jobs/default-0b6f3c1e-8f0a-4d7e-9c2b-5a1d3e4f6a70/run-main.status || true
echo "$LAUNCHER_RC" > /scratch/u/interlink/jobs/default-7c9e2a4b-1d3f-4e5a-8b6c-9d0e1f2a3b4c/run-main.status || true
exit "$LAUNCHER_RC"
