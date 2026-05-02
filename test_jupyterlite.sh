#!/usr/bin/env bash
# test_jupyterlite.sh — manual JupyterLite + wasm client debug harness.
#
# Starts a full Scaler cluster (object storage, scheduler on ws://, monitor,
# one worker) plus the docs HTTP server that serves the JupyterLite site and
# the wasm wheel.
#
# The scheduler binds to a ws:// address.  Both native CPython workers and the
# browser wasm client speak the same YMQ-over-WebSocket protocol, so a single
# bind address covers both.
#
# After this script exits you can open the URL printed below in a browser,
# navigate to debug_jupyterlite.ipynb, and run cells one-by-one to exercise
# the wasm client.
#
# Prerequisites:
#   - .venv is set up (uv pip install -e ".[all]" and dev deps: uv sync --group dev)
#   - wasm wheel has been built and deployed. THIRD_PARTY_DIR controls where
#     library_tool.sh writes / reads the wasm chain (defaults to ./thirdparties;
#     the devcontainer image bakes it at /opt/scaler and exports the var, so
#     the same commands work in both places):
#       source "${THIRD_PARTY_DIR:-./thirdparties}/emsdk/emsdk_env.sh"
#       source .venv/bin/activate
#       # one-time -- pyodide-build picks the xbuildenv that matches its pin
#       pyodide xbuildenv install
#       export CMAKE_PREFIX_PATH="${THIRD_PARTY_DIR:-$PWD/thirdparties}/wasm/install"
#       export CapnProto_DIR="${CMAKE_PREFIX_PATH}/lib/cmake/CapnProto"
#       pyodide build . --outdir dist_wasm
#       # pyodide-build 0.34.x repacks with pyemscripten_2025_0 tag; retag back
#       # to emscripten_4_0_9 for Pyodide 0.29.3's micropip:
#       uv pip install wheel && \
#         python -m wheel tags --python-tag cp313 --abi-tag cp313 \
#           --platform-tag emscripten_4_0_9_wasm32 \
#           dist_wasm/opengris_scaler-*pyemscripten*wasm32.whl
#       mkdir -p docs/build/html/_static/wasm
#       cp dist_wasm/opengris_scaler-*emscripten_4_0_9*wasm32.whl docs/build/html/_static/wasm/
#   - docs have been built:  cd docs && make html
#   - tmux is installed

set -euo pipefail

SESSION="scaler-jl"
VENV="/workspaces/scaler/.venv/bin/activate"

# Ports
OBJECT_STORAGE_PORT=7379
SCHEDULER_WS_PORT=7380    # workers + browser wasm client both use this
MONITOR_PORT=7381
DOCS_PORT=8765

OBJECT_STORAGE_ADDR="tcp://127.0.0.1:${OBJECT_STORAGE_PORT}"
SCHEDULER_WS_ADDR="ws://0.0.0.0:${SCHEDULER_WS_PORT}"
SCHEDULER_WS_CLIENT_ADDR="ws://127.0.0.1:${SCHEDULER_WS_PORT}"
MONITOR_ADDR="tcp://127.0.0.1:${MONITOR_PORT}"

# ws:// addresses require the YMQ network backend.  ZMQ (the default) only
# understands tcp:// / ipc:// / inproc://.
export SCALER_NETWORK_BACKEND=ymq

tmux kill-session -t "$SESSION" 2>/dev/null || true

echo "Starting Scaler cluster for JupyterLite testing..."

# 1. Object storage server
tmux new-session -d -s "$SESSION" -n object_storage
tmux send-keys -t "$SESSION:object_storage" \
    "source $VENV && scaler_object_storage_server $OBJECT_STORAGE_ADDR" Enter

sleep 1

# 2. Scheduler — ws:// so both native workers and browser wasm client can connect.
tmux new-window -t "$SESSION" -n scheduler
tmux send-keys -t "$SESSION:scheduler" \
    "source $VENV && scaler_scheduler $SCHEDULER_WS_ADDR -osa $OBJECT_STORAGE_ADDR -ma $MONITOR_ADDR" Enter

sleep 1

# 3. Monitor UI
tmux new-window -t "$SESSION" -n ui
tmux send-keys -t "$SESSION:ui" \
    "source $VENV && scaler_gui $MONITOR_ADDR" Enter

sleep 2

# 4. One worker — also connects via ws://
tmux new-window -t "$SESSION" -n worker
tmux send-keys -t "$SESSION:worker" \
    "source $VENV && scaler_worker_manager baremetal_native $SCHEDULER_WS_CLIENT_ADDR --worker-manager-id jl_worker --max-task-concurrency 4" Enter

sleep 2

# 5. Docs HTTP server — serves JupyterLite + wasm wheel
tmux new-window -t "$SESSION" -n docs_server
tmux send-keys -t "$SESSION:docs_server" \
    "cd /workspaces/scaler/docs/build/html && python -m http.server $DOCS_PORT" Enter

sleep 1

echo ""
echo "======================================================================"
echo "  JupyterLite debug environment ready"
echo "======================================================================"
echo ""
echo "  Scaler monitor UI  : http://localhost:50001"
echo "  JupyterLite site   : http://localhost:${DOCS_PORT}/lite/lab/index.html"
echo "  Debug notebook     : http://localhost:${DOCS_PORT}/lite/lab/index.html?path=debug_jupyterlite.ipynb"
echo ""
echo "  Scheduler (workers + browser wasm): ${SCHEDULER_WS_CLIENT_ADDR}"
echo ""
echo "  NOTE: set SCHEDULER_ADDRESS = '${SCHEDULER_WS_CLIENT_ADDR}' in the notebook."
echo "        The wasm wheel must be at docs/build/html/_static/wasm/ before running."
echo ""
echo "  To attach to tmux : tmux attach -t $SESSION"
echo "  To stop everything: tmux kill-session -t $SESSION"
echo "======================================================================"
