# Pixels C++ Implementation

## Usage

### Build Source Code

Pixels C++ relies on protobuf, liburing, boost, and googletest. Furthermore, it builds the pixels extension of duckdb by default,
which relies on [duckdb](https://github.com/pixelsdb/duckdb).
We don't need to manually install these prerequisites, since the build scripts would automatically download them.

Pixels C++ reader uses `iouring` system calls. You can use the following command to check if iouring is supported in your system:

```shell
grep io_uring_setup /proc/kallsyms
```

If the command outputs the following information, it means your system supports `iouring`. Otherwise, please upgrade the system.
```shell
0000000000000000 t io_uring_setup
0000000000000000 T __x64_sys_io_uring_setup
0000000000000000 T __ia32_sys_io_uring_setup
0000000000000000 d event_exit__io_uring_setup
0000000000000000 d event_enter__io_uring_setup
0000000000000000 d __syscall_meta__io_uring_setup
0000000000000000 d args__io_uring_setup
0000000000000000 d types__io_uring_setup
0000000000000000 d __event_exit__io_uring_setup
0000000000000000 d __event_enter__io_uring_setup
0000000000000000 d __p_syscall_meta__io_uring_setup
0000000000000000 d _eil_addr___ia32_sys_io_uring_setup
0000000000000000 d _eil_addr___x64_sys_io_uring_setup
```

Then set the `PIXELS_SRC` and `PIXELS_HOME` environment variable (Ignore it if you have already set it when installing Pixels). `PIXELS_SRC` is the source directory of pixels (e.g. `/home/liyu/pixels`). It is used to locate proto files. `PIXELS_HOME/etc` is the path where `pixels-cpp.properties` locates. `PIXELS_HOME` is usually the same as the installation path of pixels.

Pull the dependency code:

```shell
make pull
```

If it is not the first time to execute `make pull`, you can check out the latest submodules:

```shell
make update
```

Finally, compile the code:

```shell
make -j
```

### Run Basic Example

Here is a pixels reader example in the directory `duckdb/examples/pixels-example`.
This example validates the correctness of compilation and gives you an idea how to load the Pixels data. 

To run this binary:

```
./build/release/examples/pixels-example/pixels-example
```

### Run pixels C++ in CLion
In order to run the code in CLion, we should set the cmake configuration in CLion. 

In `"setting"`->`"Build,Execution,Deployment"`->`"CMake"`, set `"Generator"` as `"Let CMake decide"`, and
set `CMake options` as:
```shell
-DDUCKDB_EXTENSION_NAMES="pixels" -DDUCKDB_EXTENSION_PIXELS_PATH=${PIXELS_SRC}/cpp -DDUCKDB_EXTENSION_PIXELS_SHOULD_LINK="TRUE" -DDUCKDB_EXTENSION_PIXELS_INCLUDE_PATH=${PIXELS_SRC}/cpp/include -DCMAKE_PREFIX_PATH=${PIXELS_SRC}/cpp/third-party/protobuf/cmake/build
```

### Run Benchmarks

Note: the benchmark runs on diascld31 server. If you run the benchmark on your machine, please refer to [Common issue](#4-i-fail-to-run-the-pixels-and-parquet-benchmark)

#### 1. Simple query
We run one simple query by the following command:

```
build/release/benchmark/benchmark_runner "benchmark/tpch/pixels/tpch_1/q01.benchmark"
```
#### 2. TPC-H benchmark
The benchmark script is `run_benchmark.py` in `duckdb/scripts` directory.

Check the usage:

```
cd pixels-duckdb/duckdb
python scripts/run_benchmark.py --help
```

Run TPC-H 1 benchmarks:

```
cd pixels-duckdb/duckdb
python scripts/run_benchmark.py --pixels "benchmark/tpch/pixels/tpch_1/" --parquet "benchmark/tpch/parquet/tpch_1/" -v --repeat-time-disk 1
```

or enabling the encoding of pixels:

```
cd pixels-duckdb/duckdb
python scripts/run_benchmark.py --pixels "benchmark/tpch/pixels/tpch_1_encoding/" --parquet "benchmark/tpch/parquet/tpch_1/" -v --repeat-time-disk 1
```

Run TPCH 300 benchmark:

```
cd pixels-duckdb/duckdb
python scripts/run_benchmark.py --pixels "benchmark/tpch/pixels/tpch_300/" --parquet "benchmark/tpch/parquet/tpch_300/" -v --repeat-time-disk 1
```

or enabling the encoding of pixels:

```
cd pixels-duckdb/duckdb
python scripts/run_benchmark.py --pixels "benchmark/tpch/pixels/tpch_300_encoding/" --parquet "benchmark/tpch/parquet/tpch_300/" -v --repeat-time-disk 1
```

We also support to just run pixels or parquet:

```
cd pixels-duckdb/duckdb
python scripts/run_benchmark.py --pixels "benchmark/tpch/pixels/tpch_1/" -v --repeat-time-disk 1
```

We also offer self-defined queries:

```
cd pixels-duckdb/duckdb
python scripts/run_benchmark.py --pixels "benchmark/tpch/pixels/micro-benchmark/tpch_1/" -v  --repeat-time-disk 1
```

The execution time plot is generated in `cpp/plot` directory.


### SSD array experiment

SSD array needs pixels data to be in multiple directories. The following instructions show an example to run benchmarks on SSD array:

#### 1. split pixels data to multiple SSDs

We offer a convenient python script to distribute the pixels data in a single directory to multiple directories. For example:

```
cd pixels-duckdb/duckdb
python scripts/pixels-multidir-generator.py -i /data/s1725-1/liyu/pixels_data/pixels-tpch-1-small-endian \
-o /data/s1725-1/liyu/pixels_data/pixels-tpch-1-small-endian-partition1 \
/data/s1725-1/liyu/pixels_data/pixels-tpch-1-small-endian-partition2 \
/data/s1725-1/liyu/pixels_data/pixels-tpch-1-small-endian-partition3 \
/data/s1725-1/liyu/pixels_data/pixels-tpch-1-small-endian-partition4 -v
```

In this example we only show the usage of the python script. Remember to replace the output directory to the directories in different SSDs.

The python script copies files from the input directory to all output paths. The pixels file names are collected from the input directory, sorted by the file index 
(e.g. the file index of `20230809035030_4630.pxl` is 4630) and then copied to the output paths in the round-rubin fashion. Pixels C++ reader reads 
the pixels data sorted by the file index, so all SSDs can be utilized simultaneously.

#### 2. Modify the benchmark

The above python script outputs the following queries:

```shell
----------Copy the below commands to the benchmark template ----------
CREATE VIEW orders AS SELECT * FROM pixels_scan(["/data/s1725-1/liyu/pixels_data/pixels-tpch-1-small-endian-partition1/orders/v-0-ordered/*.pxl","/data/s1725-1/liyu/pixels_data/pixels-tpch-1-small-endian-partition2/orders/v-0-ordered/*.pxl","/data/s1725-1/liyu/pixels_data/pixels-tpch-1-small-endian-partition3/orders/v-0-ordered/*.pxl","/data/s1725-1/liyu/pixels_data/pixels-tpch-1-small-endian-partition4/orders/v-0-ordered/*.pxl"]);
CREATE VIEW customer AS SELECT * FROM pixels_scan(["/data/s1725-1/liyu/pixels_data/pixels-tpch-1-small-endian-partition1/customer/v-0-ordered/*.pxl","/data/s1725-1/liyu/pixels_data/pixels-tpch-1-small-endian-partition2/customer/v-0-ordered/*.pxl","/data/s1725-1/liyu/pixels_data/pixels-tpch-1-small-endian-partition3/customer/v-0-ordered/*.pxl","/data/s1725-1/liyu/pixels_data/pixels-tpch-1-small-endian-partition4/customer/v-0-ordered/*.pxl"]);
CREATE VIEW nation AS SELECT * FROM pixels_scan(["/data/s1725-1/liyu/pixels_data/pixels-tpch-1-small-endian-partition1/nation/v-0-ordered/*.pxl","/data/s1725-1/liyu/pixels_data/pixels-tpch-1-small-endian-partition2/nation/v-0-ordered/*.pxl","/data/s1725-1/liyu/pixels_data/pixels-tpch-1-small-endian-partition3/nation/v-0-ordered/*.pxl","/data/s1725-1/liyu/pixels_data/pixels-tpch-1-small-endian-partition4/nation/v-0-ordered/*.pxl"]);
CREATE VIEW part AS SELECT * FROM pixels_scan(["/data/s1725-1/liyu/pixels_data/pixels-tpch-1-small-endian-partition1/part/v-0-ordered/*.pxl","/data/s1725-1/liyu/pixels_data/pixels-tpch-1-small-endian-partition2/part/v-0-ordered/*.pxl","/data/s1725-1/liyu/pixels_data/pixels-tpch-1-small-endian-partition3/part/v-0-ordered/*.pxl","/data/s1725-1/liyu/pixels_data/pixels-tpch-1-small-endian-partition4/part/v-0-ordered/*.pxl"]);
CREATE VIEW partsupp AS SELECT * FROM pixels_scan(["/data/s1725-1/liyu/pixels_data/pixels-tpch-1-small-endian-partition1/partsupp/v-0-ordered/*.pxl","/data/s1725-1/liyu/pixels_data/pixels-tpch-1-small-endian-partition2/partsupp/v-0-ordered/*.pxl","/data/s1725-1/liyu/pixels_data/pixels-tpch-1-small-endian-partition3/partsupp/v-0-ordered/*.pxl","/data/s1725-1/liyu/pixels_data/pixels-tpch-1-small-endian-partition4/partsupp/v-0-ordered/*.pxl"]);
CREATE VIEW lineitem AS SELECT * FROM pixels_scan(["/data/s1725-1/liyu/pixels_data/pixels-tpch-1-small-endian-partition1/lineitem/v-0-ordered/*.pxl","/data/s1725-1/liyu/pixels_data/pixels-tpch-1-small-endian-partition2/lineitem/v-0-ordered/*.pxl","/data/s1725-1/liyu/pixels_data/pixels-tpch-1-small-endian-partition3/lineitem/v-0-ordered/*.pxl","/data/s1725-1/liyu/pixels_data/pixels-tpch-1-small-endian-partition4/lineitem/v-0-ordered/*.pxl"]);
CREATE VIEW supplier AS SELECT * FROM pixels_scan(["/data/s1725-1/liyu/pixels_data/pixels-tpch-1-small-endian-partition1/supplier/v-0-ordered/*.pxl","/data/s1725-1/liyu/pixels_data/pixels-tpch-1-small-endian-partition2/supplier/v-0-ordered/*.pxl","/data/s1725-1/liyu/pixels_data/pixels-tpch-1-small-endian-partition3/supplier/v-0-ordered/*.pxl","/data/s1725-1/liyu/pixels_data/pixels-tpch-1-small-endian-partition4/supplier/v-0-ordered/*.pxl"]);
CREATE VIEW region AS SELECT * FROM pixels_scan(["/data/s1725-1/liyu/pixels_data/pixels-tpch-1-small-endian-partition1/region/v-0-ordered/*.pxl","/data/s1725-1/liyu/pixels_data/pixels-tpch-1-small-endian-partition2/region/v-0-ordered/*.pxl","/data/s1725-1/liyu/pixels_data/pixels-tpch-1-small-endian-partition3/region/v-0-ordered/*.pxl","/data/s1725-1/liyu/pixels_data/pixels-tpch-1-small-endian-partition4/region/v-0-ordered/*.pxl"]);
```

We replace these `CREATE` queries in `pixels_tpch_template.benchmark.in`, and run the benchmark:

```
cd pixels-duckdb/duckdb
python scripts/run_benchmark.py --pixels "benchmark/tpch/pixels/tpch_1/" -v --repeat-time-disk 1
```

## Parameters

Here are two important parameters in `pixels-cpp.properties`:

* `localfs.enable.direct.io`: use DIRECT IO or buffer IO

* `localfs.enable.async.io`: use async IO or sync IO

* `pixel.stride`: pixels stride. Pixels C++ reader uses this number as the batch size, so make sure it is the same as the pixels stride in the pixels java writer.

Currently, the following configuration has the best performance in pixels C++ reader:

```
localfs.enable.direct.io=true
localfs.enable.async.io=true
```


## Common issues

### 1. How to fetch the lastest pixels reader and duckdb?

`pixels reader` and `duckdb` will be updated frequently in the next few months, so please keep the two submodules updated. 

To fetch the latest pixels reader and duckdb:

```
make update
```


### 2. The compilation fails in duckdb

Please make sure you don't use the official `duckdb` repository. The official `duckdb` has some name conflicts with `iouring` (which is a linux async IO library), which would lead to the compilation failure.

### 3. I fail to run the pixels and parquet benchmark

This code is tested in diascld31 server. I hardcode the pixels data directory and parquet data directory in `parquet_tpch_template.benchmark.in`, `parquet_tpch_template_no_verification.benchmark.in`, `pixels_tpch_template.benchmark.in` and `pixels_tpch_template_no_verification.benchmark.in`. If you want to run this benchmark in another machine, make sure to modify the pixels and parquet directory to the correct location. `TODO`: In the future I will rewrite this to a more user-friendly benchmark.

Also, since I have no sudo permission on diascld31 server, I have to clean the page cache by running the command:

```
sudo /scratch/pixels-external/drop_cache.sh
``` 

My sudo permission is solely on this shell script. You can find this command in `run_benchmark.py`. 

Specifically, the shell script `drop_cache.sh` is just one command:

```
sudo bash -c "sync; echo 3 > /proc/sys/vm/drop_caches"
```

Therefore, in order to run the benchmark in another machine, you need to change the `clean_page_cache` function in the python script to the following one:

```
def clean_page_cache():
    cmd = "sudo bash -c "sync; echo 3 > /proc/sys/vm/drop_caches""
    if verbose:
        print(cmd)
    os.system(cmd)
```

### 4. The protobuf version issue
We use protobuf [v3.21.6](https://github.com/protocolbuffers/protobuf/releases/tag/v3.21.6). It is pulled as a submodule. The latest protobuf version doesn't work for pixels c++ reader. 

### 5. Boost C++ Libraries
We use boost [1.74.0](https://github.com/boostorg/boost/tree/boost-1.74.0) that requires CMake 3.19 or later.
Boost is downloaded automatically in the CMakeLists of pixels-cli.
