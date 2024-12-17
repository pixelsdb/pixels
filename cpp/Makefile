.PHONY: all clean debug release pull update deps

all: release

MKFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
PROJ_DIR := $(dir $(MKFILE_PATH))

OSX_BUILD_UNIVERSAL_FLAG=
ifeq (${OSX_BUILD_UNIVERSAL}, 1)
	OSX_BUILD_UNIVERSAL_FLAG=-DOSX_BUILD_UNIVERSAL=1
endif
ifeq (${STATIC_LIBCPP}, 1)
	STATIC_LIBCPP=-DSTATIC_LIBCPP=TRUE
endif

ifeq ($(GEN),ninja)
	GENERATOR=-G "Ninja"
	FORCE_COLOR=-DFORCE_COLORED_OUTPUT=1
endif

PROTOBUF_DIR=third-party/protobuf
BUILD_FLAGS=-DEXTENSION_STATIC_BUILD=1 -DBUILD_TPCH_EXTENSION=1 -DBUILD_BENCHMARKS=1 -DBUILD_PARQUET_EXTENSION=1 \
${OSX_BUILD_UNIVERSAL_FLAG} ${STATIC_LIBCPP}

CLIENT_FLAGS :=

# These flags will make DuckDB build the extension

EXTENSION_FLAGS=-DDUCKDB_EXTENSION_NAMES="pixels" -DDUCKDB_EXTENSION_PIXELS_PATH="$(PROJ_DIR)" \
-DDUCKDB_EXTENSION_PIXELS_SHOULD_LINK="TRUE" -DDUCKDB_EXTENSION_PIXELS_INCLUDE_PATH="$(PROJ_DIR)include" \
-DCMAKE_PREFIX_PATH=$(PROJ_DIR)third-party/protobuf/cmake/build -DPIXELS_SRC="$(dirname $(pwd))"

pull:
	git submodule init
	git submodule update --recursive --init

update:
	git submodule update --remote --merge

deps:
	mkdir -p "${PROTOBUF_DIR}/cmake/build" && cd "third-party/protobuf/cmake/build" && \
	cmake -Dprotobuf_BUILD_TESTS=OFF -DCMAKE_BUILD_TYPE=Release ../.. -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
		-Dprotobuf_BUILD_SHARED_LIBS=ON -DCMAKE_INSTALL_PREFIX=./ && \
	make -j install

clean:
	rm -rf build
	cd pixels-duckdb && make clean

# Main build
debug: deps
	cmake $(GENERATOR) $(FORCE_COLOR) $(EXTENSION_FLAGS) ${CLIENT_FLAGS} -DEXTENSION_STATIC_BUILD=1 -DCMAKE_BUILD_TYPE=Debug ${BUILD_FLAGS} -S pixels-duckdb/ -B build/debug && \
	cmake --build build/debug --config Debug

release: deps
	cmake $(GENERATOR) $(FORCE_COLOR) $(EXTENSION_FLAGS) ${CLIENT_FLAGS} -DEXTENSION_STATIC_BUILD=1 -DCMAKE_BUILD_TYPE=Release ${BUILD_FLAGS} -S pixels-duckdb/ -B build/release && \
	cmake --build build/release --config Release