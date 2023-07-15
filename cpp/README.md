# Pixels Reader C++ Implementation

## Usage


### Prerequisites

Install tbb:
```shell
echo "deb http://cz.archive.ubuntu.com/ubuntu eoan main universe" | sudo tee -a  /etc/apt/sources.list
sudo apt-get update
sudo apt-get install -y libtbb-dev
```

Install protobuf and iouring:
```shell
sudo apt-get install -y protobuf-compiler liburing-dev
```

### Compilation

The repository relies on [duckdb](https://github.com/yuly16/duckdb). It is refered from [pixels reader](https://github.com/yuly16/pixels-reader-cxx). 

First fetch `duckdb` submodules from github:

```
make pull
```


Finally compile the code:

```
make -j
```

### Example

Here is an pixels reader example in the directory `duckdb/examples/pixels-example`. This example validates the correctness of compilation and gives you an idea how to load the pixels data. 

To run this binary:

```
./build/release/examples/pixels-example/pixels-example
```

### Benchmark

Note: the benchmark runs on diascld31 server. If you run the benchmark on your machine, please refer to [Common issue](#4-i-fail-to-run-the-pixels-and-parquet-benchmark)

#### 1. Simple query
We run one simple query by the following command:

```
build/release/benchmark/benchmark_runner "benchmark/tpch/pixels/tpch_1/q01.benchmark"
```
#### 2. TPCH benchmark
The benchmark script is `run_benchmark.py` in `duckdb/scripts` directory.

Check the usage:

```
cd duckdb
python scripts/run_benchmark.py --help
```

Run TPCH 1 benchmarks:

```
python scripts/run_benchmark.py --pixels "benchmark/tpch/pixels/tpch_1/" --parquet "benchmark/tpch/parquet/tpch_1/" -v --repeat-time-disk 1
```

or enabling the encoding of pixels:

```
python scripts/run_benchmark.py --pixels "benchmark/tpch/pixels/tpch_1_encoding/" --parquet "benchmark/tpch/parquet/tpch_1/" -v --repeat-time-disk 1
```

Run TPCH 300 benchmark:

```
python scripts/run_benchmark.py --pixels "benchmark/tpch/pixels/tpch_300/" --parquet "benchmark/tpch/parquet/tpch_300/" -v --repeat-time-disk 1
```

or enabling the encoding of pixels:

```
python scripts/run_benchmark.py --pixels "benchmark/tpch/pixels/tpch_300_encoding/" --parquet "benchmark/tpch/parquet/tpch_300/" -v --repeat-time-disk 1
```

We also support to just run pixels or parquet:

```
python scripts/run_benchmark.py --pixels "benchmark/tpch/pixels/tpch_1/" -v --repeat-time-disk 1
```

We also offer self-defined queries:

```
python scripts/run_benchmark.py --pixels "benchmark/tpch/pixels/micro-benchmark/tpch_1/" -v  --repeat-time-disk 1
```

The execution time plot is generated in `plot` directory.


## Parameters

Here are two important parameters in `pixels.properties`: 

* `localfs.enable.direct.io`: use DIRECT IO or buffer IO

* `localfs.enable.async.io`: use async IO or sync IO

Currently the following configuration has the best performance in pixels C++ reader:

```
localfs.enable.direct.io=true
localfs.enable.async.io=true
```


## Common issues

### 1. How to fetch the lastest pixels reader and duckdb?

`pixels reader` and `duckdb` will be updated frequently in the next few months, so please keep the two submodules updated. 

To fetch the lastest pixels reader and duckdb:

```
make update
```


### 2. The compilation fails in duckdb

Please make sure you don't use the official `duckdb` repository. The official `duckdb` has some name conflicts with `iouring` (which is a linux async IO library), which would lead to the compilation failure. 


### 3. I can't load the pixels data via pixels C++ reader

Currently the pixels Java writer and reader uses big endian to write/read pixels data. We find that small endian is more efficient for pixels c++ reader. Therefore, in order to generate the pixels data with small endian, please use Pixels in [little-endian branch](https://github.com/pixelsdb/pixels/tree/little-endian). We will merge the small endian to pixels java reader in the future. 


### 4. I fail to run the pixels and parquet benchmark

This code is tested in diascld31 server. I hardcode the pixels data path and parquet data path in `parquet_tpch_template.benchmark.in`, `parquet_tpch_template_no_verification.benchmark.in`, `pixels_tpch_template.benchmark.in` and `pixels_tpch_template_no_verification.benchmark.in`. If you want to run this benchmark in another machine, make sure to modify the pixels and parquet path to the correct location. `TODO`: In the future I will rewrite this to a more user-friendly benchmark.

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
