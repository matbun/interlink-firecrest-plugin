# The FirecREST demo cluster (image "slurm", built by firecrest-v2's
# docker-compose.yml) plus Apptainer, so the jobs the slurm plugin writes can
# run their containers.
FROM slurm

RUN apt-get update \
  && apt-get install -y --no-install-recommends software-properties-common gpg-agent \
  && add-apt-repository -y ppa:apptainer/ppa \
  && apt-get update \
  && apt-get install -y --no-install-recommends apptainer squashfs-tools \
  && ln -sf /usr/bin/apptainer /usr/local/bin/singularity \
  && rm -rf /var/lib/apt/lists/*
