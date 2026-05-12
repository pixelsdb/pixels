#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
BUILD_DIR="${SCRIPT_DIR}/build"
AWS_SDK_VERSION="${AWS_SDK_VERSION:-1.11.578}"
AWS_INSTALL_PREFIX="${AWS_INSTALL_PREFIX:-/usr/local}"
WORK_DIR="${SCRIPT_DIR}/.deps"
AWS_WORK_DIR="${WORK_DIR}/aws-sdk-cpp"
GFLAGS_WORK_DIR="${WORK_DIR}/gflags"
ZSTD_WORK_DIR="${WORK_DIR}/zstd"

SUDO=""
if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
  SUDO="sudo"
fi

note() {
  echo "[pixels-index-rockset-ci] $*"
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

run() {
  note "$*"
  "$@"
}

require_command() {
  local cmd="$1"
  command -v "$cmd" >/dev/null 2>&1 || die "Required command '$cmd' was not found in PATH."
}

require_file() {
  local path="$1"
  local hint="${2:-}"
  if [[ ! -f "$path" ]]; then
    if [[ -n "$hint" ]]; then
      die "Missing required file '$path'. $hint"
    fi
    die "Missing required file '$path'."
  fi
}

require_dir() {
  local path="$1"
  local hint="${2:-}"
  if [[ ! -d "$path" ]]; then
    if [[ -n "$hint" ]]; then
      die "Missing required directory '$path'. $hint"
    fi
    die "Missing required directory '$path'."
  fi
}

version_ge() {
  local current="$1"
  local minimum="$2"
  [[ "$(printf '%s\n%s\n' "$minimum" "$current" | sort -V | head -n1)" == "$minimum" ]]
}

check_compiler() {
  local compiler_path="$1"
  local compiler_name="$2"
  local compiler_version

  [[ -n "$compiler_path" ]] || die "Required compiler '$compiler_name' was not found in PATH."
  [[ -x "$compiler_path" ]] || die "Compiler '$compiler_path' is not executable."

  compiler_version=$("$compiler_path" -dumpversion 2>/dev/null | cut -d. -f1,2)
  [[ -n "$compiler_version" ]] || die "Unable to determine version for compiler '$compiler_path'."

  version_ge "$compiler_version" "4.8" || die \
    "Compiler '$compiler_path' is version $compiler_version. GCC/G++ 4.8 or later is required for C++11 support."
}

find_aws_lib_dir() {
  local candidate
  for candidate in "${AWS_INSTALL_PREFIX}/lib" "${AWS_INSTALL_PREFIX}/lib64"; do
    if [[ -f "${candidate}/libaws-cpp-sdk-core.so" ]]; then
      echo "$candidate"
      return 0
    fi
  done
  return 1
}

find_library_dir() {
  local lib_name="$1"
  local candidate
  for candidate in \
    /usr/lib \
    /usr/lib64 \
    /usr/local/lib \
    /usr/local/lib64 \
    /lib/x86_64-linux-gnu \
    /usr/lib/x86_64-linux-gnu \
    /lib/aarch64-linux-gnu \
    /usr/lib/aarch64-linux-gnu; do
    if [[ -f "${candidate}/${lib_name}" ]]; then
      echo "$candidate"
      return 0
    fi
  done
  return 1
}

find_header_file() {
  local header_path="$1"
  local candidate
  for candidate in \
    /usr/include \
    /usr/local/include \
    /usr/include/x86_64-linux-gnu \
    /usr/local/include/x86_64-linux-gnu \
    /usr/include/aarch64-linux-gnu \
    /usr/local/include/aarch64-linux-gnu; do
    if [[ -f "${candidate}/${header_path}" ]]; then
      echo "${candidate}/${header_path}"
      return 0
    fi
  done
  return 1
}

check_system_dependency() {
  local header="$1"
  local library="$2"
  local dependency_name="$3"
  local install_hint="$4"

  find_header_file "$header" >/dev/null || die \
    "Missing header '$header' for ${dependency_name}. ${install_hint}"
  find_library_dir "$library" >/dev/null || die \
    "Missing library '$library' for ${dependency_name}. ${install_hint}"
}

check_linux_build_deps() {
  check_system_dependency \
    "gflags/gflags.h" \
    "libgflags.so" \
    "gflags" \
    "On Ubuntu, install 'libgflags-dev'. On CentOS/RHEL, build gflags v2.0 from source and expose its include/lib paths through CPATH and LIBRARY_PATH if installed under /usr/local."
  check_system_dependency \
    "snappy.h" \
    "libsnappy.so" \
    "snappy" \
    "On Ubuntu, install 'libsnappy-dev'. On CentOS/RHEL, install 'snappy snappy-devel'."
  check_system_dependency \
    "zlib.h" \
    "libz.so" \
    "zlib" \
    "On Ubuntu, install 'zlib1g-dev'. On CentOS/RHEL, install 'zlib zlib-devel'."
  check_system_dependency \
    "bzlib.h" \
    "libbz2.so" \
    "bzip2" \
    "On Ubuntu, install 'libbz2-dev'. On CentOS/RHEL, install 'bzip2 bzip2-devel'."
  check_system_dependency \
    "lz4.h" \
    "liblz4.so" \
    "lz4" \
    "On Ubuntu, install 'liblz4-dev'. On CentOS/RHEL, install 'lz4-devel'."
  check_system_dependency \
    "zstd.h" \
    "libzstd.so" \
    "zstandard" \
    "On Ubuntu, install 'libzstd-dev'. On CentOS/RHEL, build zstd from source or install the distro-provided development package if available."
  check_system_dependency \
    "curl/curl.h" \
    "libcurl.so" \
    "libcurl" \
    "On Ubuntu, install 'libcurl4-openssl-dev'. On CentOS/RHEL, install the libcurl development package for your distribution."
}

check_aws_sdk() {
  local aws_include="${AWS_INSTALL_PREFIX}/include/aws/core/Aws.h"
  local aws_lib_dir

  require_file \
    "$aws_include" \
    "Install AWS SDK for C++ to ${AWS_INSTALL_PREFIX}. See README.md for the exact commands."

  aws_lib_dir=$(find_aws_lib_dir) || die \
    "AWS SDK libraries were not found under ${AWS_INSTALL_PREFIX}/lib or ${AWS_INSTALL_PREFIX}/lib64. See README.md for the expected installation steps."

  require_file "${aws_lib_dir}/libaws-cpp-sdk-core.so"
  require_file "${aws_lib_dir}/libaws-cpp-sdk-s3.so"
  require_file "${aws_lib_dir}/libaws-cpp-sdk-transfer.so"
  require_file "${aws_lib_dir}/libaws-cpp-sdk-kinesis.so"
}

source_os_release() {
  [[ -f /etc/os-release ]] || die "Cannot detect Linux distribution: /etc/os-release is missing."
  # shellcheck disable=SC1091
  source /etc/os-release
}

is_ubuntu_like() {
  [[ "${ID:-}" == "ubuntu" ]] || [[ "${ID_LIKE:-}" == *"debian"* ]]
}

is_rhel_like() {
  [[ "${ID:-}" == "centos" ]] || [[ "${ID:-}" == "rhel" ]] || [[ "${ID:-}" == "rocky" ]] || [[ "${ID:-}" == "almalinux" ]] || [[ "${ID_LIKE:-}" == *"rhel"* ]] || [[ "${ID_LIKE:-}" == *"fedora"* ]]
}

pick_pkg_manager() {
  if command -v dnf >/dev/null 2>&1; then
    echo "dnf"
    return 0
  fi
  if command -v yum >/dev/null 2>&1; then
    echo "yum"
    return 0
  fi
  die "Neither dnf nor yum is available."
}

install_ubuntu_packages() {
  run ${SUDO} apt-get update
  run ${SUDO} apt-get install -y \
    build-essential \
    cmake \
    git \
    curl \
    pkg-config \
    libgflags-dev \
    libsnappy-dev \
    zlib1g-dev \
    libbz2-dev \
    liblz4-dev \
    libzstd-dev \
    libcurl4-openssl-dev
}

install_rhel_packages() {
  local pkg_manager
  pkg_manager=$(pick_pkg_manager)

  run ${SUDO} "${pkg_manager}" install -y \
    gcc \
    gcc-c++ \
    make \
    cmake \
    git \
    curl \
    pkgconfig \
    snappy \
    snappy-devel \
    zlib \
    zlib-devel \
    bzip2 \
    bzip2-devel \
    lz4-devel \
    which
}

ensure_gflags_rhel() {
  if [[ -f /usr/include/gflags/gflags.h || -f /usr/local/include/gflags/gflags.h ]]; then
    return 0
  fi

  mkdir -p "$WORK_DIR"
  if [[ ! -d "$GFLAGS_WORK_DIR/.git" ]]; then
    run git clone https://github.com/gflags/gflags.git "$GFLAGS_WORK_DIR"
  fi

  (
    cd "$GFLAGS_WORK_DIR"
    run git fetch --tags --force
    run git checkout v2.0
    run ./configure
    run make -j"$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 1)"
    run ${SUDO} make install
  )

  export CPATH="/usr/local/include:${CPATH:-}"
  export LIBRARY_PATH="/usr/local/lib:${LIBRARY_PATH:-}"
  export LD_LIBRARY_PATH="/usr/local/lib:${LD_LIBRARY_PATH:-}"
}

ensure_zstd_rhel() {
  if [[ -f /usr/include/zstd.h || -f /usr/local/include/zstd.h ]]; then
    return 0
  fi

  mkdir -p "$WORK_DIR"
  if [[ ! -d "$ZSTD_WORK_DIR" ]]; then
    (
      cd "$WORK_DIR"
      run curl -L -o zstd-1.1.3.tar.gz https://github.com/facebook/zstd/archive/v1.1.3.tar.gz
      run tar zxvf zstd-1.1.3.tar.gz
      run mv zstd-1.1.3 "$ZSTD_WORK_DIR"
    )
  fi

  (
    cd "$ZSTD_WORK_DIR"
    run make -j"$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 1)"
    run ${SUDO} make install
  )
}

ensure_aws_sdk() {
  if [[ -f "${AWS_INSTALL_PREFIX}/include/aws/core/Aws.h" ]] && \
     [[ -f "${AWS_INSTALL_PREFIX}/lib/libaws-cpp-sdk-core.so" || -f "${AWS_INSTALL_PREFIX}/lib64/libaws-cpp-sdk-core.so" ]]; then
    return 0
  fi

  mkdir -p "$WORK_DIR"
  if [[ ! -d "$AWS_WORK_DIR/.git" ]]; then
    run git clone https://github.com/aws/aws-sdk-cpp "$AWS_WORK_DIR"
  fi

  (
    cd "$AWS_WORK_DIR"
    run git fetch --tags --force
    run git checkout "${AWS_SDK_VERSION}"
    run git submodule update --init --recursive
    rm -rf build
    mkdir -p build
    cd build
    run cmake .. \
      -DCMAKE_BUILD_TYPE=Release \
      -DBUILD_ONLY=s3\;core\;transfer\;kinesis \
      -DENABLE_UNITY_BUILD=ON \
      -DENABLE_TESTING=OFF \
      -DBUILD_SHARED_LIBS=ON \
      -DCMAKE_INSTALL_PREFIX="${AWS_INSTALL_PREFIX}"
    run cmake --build . --config Release -j"$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 1)"
    run ${SUDO} cmake --install . --config Release
  )
}

ensure_linux_dependencies() {
  source_os_release

  if is_ubuntu_like; then
    install_ubuntu_packages
    return 0
  fi

  if is_rhel_like; then
    install_rhel_packages
    ensure_gflags_rhel
    ensure_zstd_rhel
    return 0
  fi

  die "Unsupported Linux distribution '${ID:-unknown}'. This CI script supports Ubuntu and CentOS/RHEL-like systems."
}

main() {
  local cc_bin
  local cxx_bin

  [[ -n "${PIXELS_HOME:-}" ]] || die "PIXELS_HOME is not set."
  [[ -n "${JAVA_HOME:-}" ]] || note "JAVA_HOME is not set. Provide a JDK in the environment before running this script."

  ensure_linux_dependencies
  ensure_aws_sdk

  if [[ -z "${JAVA_HOME:-}" ]] && command -v javac >/dev/null 2>&1; then
    export JAVA_HOME
    JAVA_HOME=$(dirname "$(dirname "$(readlink -f "$(command -v javac)")")")
    note "Derived JAVA_HOME=${JAVA_HOME}"
  fi

  require_command cmake
  require_command make
  require_command git

  cc_bin="${CC:-$(command -v gcc || true)}"
  cxx_bin="${CXX:-$(command -v g++ || true)}"

  require_dir "$PIXELS_HOME" "Run './install.sh' first or point PIXELS_HOME to a valid Pixels installation directory."
  require_dir "${JAVA_HOME}/include" "JAVA_HOME should point to a JDK, not a JRE."
  require_file "${JAVA_HOME}/include/jni.h" "JAVA_HOME should point to a JDK with JNI headers."

  check_compiler "$cc_bin" "gcc"
  check_compiler "$cxx_bin" "g++"
  check_linux_build_deps
  check_aws_sdk

  mkdir -p "$BUILD_DIR"

  note "Using PIXELS_HOME=$PIXELS_HOME"
  note "Using JAVA_HOME=$JAVA_HOME"
  note "Using CC=$cc_bin"
  note "Using CXX=$cxx_bin"
  note "Configuring build directory: $BUILD_DIR"

  (
    cd "$BUILD_DIR"
    CC="$cc_bin" CXX="$cxx_bin" run cmake ..
    run make -j"$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 1)"
    run cmake --install .
  )

  note "Build completed."
  note "Shared library: ${BUILD_DIR}/libpixels-index-rockset.so"
  note "Installed JNI and RocksDB runtime libraries into $PIXELS_HOME/lib"
}

main "$@"
