#!/usr/bin/env bash
# build_wasm_wheel.sh — build the wasm wheel and deploy it to the docs site.
#
# Run from the workspace root:
#   ./build_wasm_wheel.sh
#
# This script intentionally uses a dedicated Python 3.13 virtual environment
# (.venv-wasm) instead of the project's main .venv. The main devcontainer venv
# is Python 3.12, but the wasm build needs the same Python version CI uses so
# pyodide-build resolves the matching xbuildenv / Emscripten toolchain.
#
# THIRD_PARTY_DIR controls where the wasm toolchain lives (emsdk, wasm-target
# capnp/libuv). Defaults to ./thirdparties; the devcontainer sets it to
# /opt/scaler via the Dockerfile ENV.

set -euo pipefail

THIRD_PARTY_DIR="${THIRD_PARTY_DIR:-${PWD}/thirdparties}"
EMSDK_ENV="${THIRD_PARTY_DIR}/emsdk/emsdk_env.sh"
WASM_INSTALL="${THIRD_PARTY_DIR}/wasm/install"
WASM_VENV="${PWD}/.venv-wasm"

if ! command -v uv >/dev/null 2>&1; then
    echo "uv is required to create the Python 3.13 wasm build environment."
    exit 1
fi

# 1. Create / refresh the dedicated Python 3.13 wasm build venv.
uv venv "${WASM_VENV}" --python 3.13 --allow-existing
# shellcheck disable=SC1091
source "${WASM_VENV}/bin/activate"
uv pip install "pyodide-build==0.34.3" wheel

# 2. Activate emsdk.
if [[ ! -f "${EMSDK_ENV}" ]]; then
    echo "emsdk not found at ${EMSDK_ENV}."
    echo "Run: ./scripts/library_tool.sh emsdk download && compile && install"
    exit 1
fi
# shellcheck disable=SC1090
source "${EMSDK_ENV}"

# 3. Install the matching xbuildenv for pyodide-build 0.34.3. With Python 3.13
#    this resolves to the same 0.29.3 environment CI uses.
pyodide xbuildenv install

# 4. Point cmake at the wasm-target capnp/libuv install.
if [[ ! -d "${WASM_INSTALL}" ]]; then
    echo "Wasm libraries not found at ${WASM_INSTALL}."
    echo "Run: ./scripts/library_tool.sh capnp/libuv download/compile/install --target=wasm"
    exit 1
fi
export CMAKE_PREFIX_PATH="${WASM_INSTALL}"
export CapnProto_DIR="${WASM_INSTALL}/lib/cmake/CapnProto"

# 5. Build. Default to a single CMake job on low-memory machines.
rm -rf dist_wasm
CMAKE_BUILD_PARALLEL_LEVEL="${CMAKE_BUILD_PARALLEL_LEVEL:-1}" pyodide build . --outdir dist_wasm

# 6. pyodide-build 0.34.x tags wheels as pyemscripten_2025_0; Pyodide 0.29.3's
#    micropip expects emscripten_4_0_9_wasm32. Retag the freshly built wheel.
python -m wheel tags \
    --python-tag cp313 --abi-tag cp313 \
    --platform-tag emscripten_4_0_9_wasm32 \
    dist_wasm/opengris_scaler-*pyemscripten*wasm32.whl

# 7. Deploy to docs.
mkdir -p docs/build/html/_static/wasm
cp dist_wasm/opengris_scaler-*emscripten_4_0_9*wasm32.whl docs/build/html/_static/wasm/
echo ""
echo "Wheel deployed to docs/build/html/_static/wasm/"
echo "Run test_jupyterlite.sh to start the cluster."
