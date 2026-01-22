#!/bin/bash
set -e

# Ensure capnp libraries are findable
export LD_LIBRARY_PATH="/usr/local/lib:${LD_LIBRARY_PATH}"

PYTHON_VERSION="${PYTHON_VERSION:-cp312-cp312}"
PYTHON_BIN="/opt/python/${PYTHON_VERSION}/bin"

echo "Building with Python: ${PYTHON_VERSION}"
echo "CMAKE_BUILD_PARALLEL_LEVEL: ${CMAKE_BUILD_PARALLEL_LEVEL:-1}"

cd /workspace

# Build the wheel
${PYTHON_BIN}/pip wheel . -w /workspace/dist_manylinux --no-deps

# Repair the wheel to make it manylinux compliant
for whl in /workspace/dist_manylinux/opengris_scaler*.whl; do
    filename=$(basename "$whl")
    if [[ "$filename" != *"manylinux"* ]]; then
        echo "Checking wheel dependencies:"
        auditwheel show "$whl"
        echo ""
        echo "Repairing wheel..."
        auditwheel repair "$whl" -w /workspace/dist_manylinux --plat manylinux_2_28_x86_64
        rm "$whl"
    fi
done

echo ""
echo "Build complete! Wheels are in dist_manylinux/"
ls -la /workspace/dist_manylinux/*.whl
