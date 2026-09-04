.PHONY: all clean debug release relWithDebInfo pull update

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

BUILD_FLAGS=-DCMAKE_CXX_STANDARD=20 -DEXTENSION_STATIC_BUILD=1 -DBUILD_TPCH_EXTENSION=1 -DBUILD_BENCHMARKS=1 -DBUILD_PARQUET_EXTENSION=1 \
${OSX_BUILD_UNIVERSAL_FLAG} ${STATIC_LIBCPP}

CLIENT_FLAGS :=

# These flags will make DuckDB build the extension

EXTENSION_FLAGS=-DDUCKDB_EXTENSION_NAMES="pixels" -DDUCKDB_EXTENSION_PIXELS_PATH="$(PROJ_DIR)" \
-DDUCKDB_EXTENSION_PIXELS_SHOULD_LINK="TRUE" -DDUCKDB_EXTENSION_PIXELS_INCLUDE_PATH="$(PROJ_DIR)include" \
-DUSE_FLATBUFFERS=ON -DPIXELS_SRC="$(shell dirname $(CURDIR))"

pull:
	git submodule init
	git submodule update --recursive --init

update:
	git submodule update --remote --merge pixels-duckdb/duckdb
	git -C third-party/googletest checkout v1.15.2

clean:
	rm -rf build/release
	rm -rf build/debug
	cd pixels-duckdb/duckdb && make clean

# Main build
debug:
	cmake $(GENERATOR) $(FORCE_COLOR) $(EXTENSION_FLAGS) ${CLIENT_FLAGS} -DEXTENSION_STATIC_BUILD=1 -DCMAKE_BUILD_TYPE=Debug ${BUILD_FLAGS} -S pixels-duckdb/duckdb -B build/debug && \
	cmake --build build/debug --config Debug

release:
	cmake $(GENERATOR) $(FORCE_COLOR) $(EXTENSION_FLAGS) ${CLIENT_FLAGS} -DEXTENSION_STATIC_BUILD=1 -DCMAKE_BUILD_TYPE=Release ${BUILD_FLAGS} -S pixels-duckdb/duckdb -B build/release && \
	cmake --build build/release --config Release

relWithDebInfo:
	cmake $(GENERATOR) $(FORCE_COLOR) $(EXTENSION_FLAGS) ${CLIENT_FLAGS} -DEXTENSION_STATIC_BUILD=1 -DCMAKE_BUILD_TYPE=RelWithDebInfo ${BUILD_FLAGS} -S pixels-duckdb/duckdb -B build/relWithDebInfo && \
	cmake --build build/relWithDebInfo --config RelWithDebInfo
