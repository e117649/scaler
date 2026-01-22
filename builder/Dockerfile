# Manylinux 2.28 wheel builder for OpenGRIS Scaler
# Based on manylinux_2_28 (glibc 2.28) for maximum compatibility
FROM quay.io/pypa/manylinux_2_28_x86_64:latest

# Install build dependencies
RUN yum install -y \
    cmake \
    pkg-config \
    gcc-toolset-13-gcc \
    gcc-toolset-13-gcc-c++ \
    libuv-devel \
    && yum clean all

# Enable GCC 13 by default and include /usr/local/lib for capnp
ENV PATH="/opt/rh/gcc-toolset-13/root/usr/bin:${PATH}"
ENV LD_LIBRARY_PATH="/usr/local/lib:/opt/rh/gcc-toolset-13/root/usr/lib64:${LD_LIBRARY_PATH}"

# Set up working directories
WORKDIR /build
RUN mkdir -p /thirdparties/downloaded /thirdparties/compiled

# Copy and run library_tool.sh to install boost and capnp
COPY scripts/library_tool.sh /library_tool.sh
RUN chmod +x /library_tool.sh \
    && /library_tool.sh boost download \
    && /library_tool.sh boost compile \
    && /library_tool.sh boost install \
    && /library_tool.sh capnp download \
    && /library_tool.sh capnp compile \
    && /library_tool.sh capnp install \
    && rm -rf /thirdparties \
    && rm /library_tool.sh \
    && echo "/usr/local/lib" > /etc/ld.so.conf.d/local.conf \
    && ldconfig

# Set environment variables for the build
ENV CMAKE_BUILD_PARALLEL_LEVEL=1
ENV CMAKE_C_COMPILER=gcc
ENV CMAKE_CXX_COMPILER=g++

# Copy build script
COPY builder/build_wheel.sh /build_wheel.sh
RUN chmod +x /build_wheel.sh

WORKDIR /workspace
ENTRYPOINT ["/build_wheel.sh"]
