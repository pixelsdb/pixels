.PHONY: all clean debug release pull update fb-release fb-debug

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

# remove protobuf, use flatbuffer instead
BUILD_FLAGS=-DEXTENSION_STATIC_BUILD=1 -DBUILD_TPCH_EXTENSION=1 -DBUILD_BENCHMARKS=1 -DBUILD_PARQUET_EXTENSION=1 \

CLIENT_FLAGS :=
PIXELS_BASE_DIR := $(shell dirname $(shell pwd))

FB_FLAGS=-DUSE_FLATBUFFERS=ON \
-DDUCKDB_EXTENSION_NAMES="pixels" \
-DDUCKDB_EXTENSION_PIXELS_PATH="$(PROJ_DIR)" \
-DDUCKDB_EXTENSION_PIXELS_SHOULD_LINK="TRUE" \
-DDUCKDB_EXTENSION_PIXELS_INCLUDE_PATH="$(PROJ_DIR)include" \
-DPIXELS_SRC="$(PIXELS_BASE_DIR)"

pull:
	git submodule init
	git submodule update --recursive --init

update:
	git submodule update --remote --merge pixels-duckdb/duckdb
	git -C third-party/googletest checkout v1.15.2

clean:
	rm -rf build/fb-release
	rm -rf build/fb-debug
	cd pixels-duckdb/duckdb && make clean

debug: fb-debug
release: fb-release

fb-release:
	cmake $(GENERATOR) $(FORCE_COLOR) $(FB_FLAGS) ${CLIENT_FLAGS} \
	-DEXTENSION_STATIC_BUILD=1 -DCMAKE_BUILD_TYPE=Release ${BUILD_FLAGS} \
	-S pixels-duckdb/duckdb -B build/release && \
	cmake --build build/release --config Release

fb-debug:
	cmake $(GENERATOR) $(FORCE_COLOR) $(FB_FLAGS) ${CLIENT_FLAGS} \
	-DEXTENSION_STATIC_BUILD=1 -DCMAKE_BUILD_TYPE=Debug ${BUILD_FLAGS} \
	-S pixels-duckdb/duckdb -B build/debug && \
	cmake --build build/debug --config Debug