#!/bin/bash
# Build manylinux wheel for OpenGRIS Scaler
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "${SCRIPT_DIR}")"
IMAGE_NAME="opengris-scaler-builder:manylinux_2_28"

# Build Docker image if needed
if [[ "$(docker images -q ${IMAGE_NAME} 2>/dev/null)" == "" ]]; then
    echo "Building Docker image..."
    docker build -t "${IMAGE_NAME}" -f "${SCRIPT_DIR}/Dockerfile" "${PROJECT_DIR}"
fi

mkdir -p "${PROJECT_DIR}/dist_manylinux"

echo "Building wheel in ${PROJECT_DIR}/dist_manylinux/"

docker run --rm \
    -v "${PROJECT_DIR}:/workspace:rw" \
    -e "PYTHON_VERSION=cp312-cp312" \
    -e "CMAKE_BUILD_PARALLEL_LEVEL=1" \
    "${IMAGE_NAME}"

echo "Done! Wheel in: ${PROJECT_DIR}/dist_manylinux/"
