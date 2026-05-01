FROM mcr.microsoft.com/devcontainers/base:noble
RUN export DEBIAN_FRONTEND=noninteractive \
    && apt-get update \
    && apt-get -y install cmake pkg-config gcc-14 g++-14 libgraphblas-dev python3-pip python3-venv pipx tmux \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-14 100 \
    && update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-14 100 \
    && sudo -u vscode pipx install uv
COPY scripts /tmp/scaler-scripts
RUN /tmp/scaler-scripts/library_tool.sh boost download \
    && /tmp/scaler-scripts/library_tool.sh boost compile \
    && /tmp/scaler-scripts/library_tool.sh boost install \
    && /tmp/scaler-scripts/library_tool.sh capnp download \
    && /tmp/scaler-scripts/library_tool.sh capnp compile \
    && /tmp/scaler-scripts/library_tool.sh capnp install \
    && /tmp/scaler-scripts/library_tool.sh libuv download \
    && /tmp/scaler-scripts/library_tool.sh libuv compile \
    && /tmp/scaler-scripts/library_tool.sh libuv install \
    && rm -rf /tmp/scaler-scripts /thirdparties
# emsdk is installed at the workspace mount path on first use (not baked into
# the image) because it lives at thirdparties/emsdk/ inside the workspace and
# its tooling references that location:
#     ./scripts/library_tool.sh emsdk download
#     ./scripts/library_tool.sh emsdk compile
#     ./scripts/library_tool.sh emsdk install
