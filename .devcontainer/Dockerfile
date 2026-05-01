FROM mcr.microsoft.com/devcontainers/base:noble
RUN export DEBIAN_FRONTEND=noninteractive \
    && apt-get update \
    && apt-get -y install cmake pkg-config gcc-14 g++-14 libgraphblas-dev python3-pip python3-venv pipx tmux \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-14 100 \
    && update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-14 100 \
    && sudo -u vscode pipx install uv

# Native deps (boost, capnp, libuv) install into /usr/local; everything wasm-
# related goes under /opt/scaler/ so it survives the workspace bind mount that
# would otherwise shadow ./thirdparties/. THIRD_PARTY_DIR redirects
# library_tool.sh's working tree (downloaded tarballs + build trees + emsdk +
# pyodide venv + wasm install prefix) under /opt/scaler.
ENV THIRD_PARTY_DIR=/opt/scaler
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
    && /tmp/scaler-scripts/library_tool.sh emsdk download \
    && /tmp/scaler-scripts/library_tool.sh emsdk compile \
    && /tmp/scaler-scripts/library_tool.sh emsdk install \
    && /tmp/scaler-scripts/library_tool.sh pyodide download \
    && /tmp/scaler-scripts/library_tool.sh pyodide compile \
    && /tmp/scaler-scripts/library_tool.sh pyodide install \
    && . /opt/scaler/emsdk/emsdk_env.sh \
    && /tmp/scaler-scripts/library_tool.sh capnp compile --target=wasm \
    && /tmp/scaler-scripts/library_tool.sh capnp install --target=wasm \
    && /tmp/scaler-scripts/library_tool.sh libuv compile --target=wasm \
    && /tmp/scaler-scripts/library_tool.sh libuv install --target=wasm \
    && rm -rf /tmp/scaler-scripts /opt/scaler/downloaded /opt/scaler/compiled /opt/scaler/wasm/src

# Make `emcc` / `emcmake` / the bundled node available in every interactive
# shell. emsdk_env.sh derives EMSDK from BASH_SOURCE, so a single sourced
# snippet picks up wherever /opt/scaler/emsdk lives.
RUN printf '. /opt/scaler/emsdk/emsdk_env.sh > /dev/null 2>&1 || true\n' \
    > /etc/profile.d/emsdk.sh

