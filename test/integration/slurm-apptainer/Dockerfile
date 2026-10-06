# The FirecREST demo cluster (image "slurm", built by firecrest-v2's
# docker-compose.yml) plus Apptainer and enroot, so the jobs the slurm plugin
# writes can run their containers with either runtime (Alps uses enroot).
FROM slurm

ARG ENROOT_VERSION=4.2.1

RUN apt-get update \
  && apt-get install -y --no-install-recommends software-properties-common gpg-agent curl ca-certificates \
  && add-apt-repository -y ppa:apptainer/ppa \
  && apt-get update \
  && apt-get install -y --no-install-recommends apptainer squashfs-tools \
  && ln -sf /usr/bin/apptainer /usr/local/bin/singularity \
  && arch=$(dpkg --print-architecture) \
  && curl -fsSL -o /tmp/enroot.deb https://github.com/NVIDIA/enroot/releases/download/v${ENROOT_VERSION}/enroot_${ENROOT_VERSION}-1_${arch}.deb \
  && curl -fsSL -o /tmp/enroot-caps.deb https://github.com/NVIDIA/enroot/releases/download/v${ENROOT_VERSION}/enroot+caps_${ENROOT_VERSION}-1_${arch}.deb \
  && apt-get install -y --no-install-recommends /tmp/enroot.deb /tmp/enroot-caps.deb \
  && rm -rf /var/lib/apt/lists/* /tmp/*.deb

# Batch jobs have no XDG_RUNTIME_DIR; keep enroot's state per user under /tmp.
RUN printf '%s\n' \
    'ENROOT_RUNTIME_PATH /tmp/enroot/runtime/user-$(id -u)' \
    'ENROOT_CACHE_PATH /tmp/enroot/cache/user-$(id -u)' \
    'ENROOT_DATA_PATH /tmp/enroot/data/user-$(id -u)' \
    >> /etc/enroot/enroot.conf
