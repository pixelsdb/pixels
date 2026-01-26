# 测试
本目录存放了所有测试

## 运行脚本
`process_sqls.py` 运行查询，需要传入benchmark参数指定要运行的benchmark,也需要指定要运行的查询
```bash
usage: process_sqls.py [-h] [--runs RUNS] [--duckdb-bin DUCKDB_BIN] [--sql-dir SQL_DIR]
                       [--output-csv OUTPUT_CSV] [--wait-after-run WAIT_AFTER_RUN]
                       [--threads THREADS] [--benchmark BENCHMARK] [--benchmark-json BENCHMARK_JSON]

DuckDB ClickBench Batch Test Script (Multi-column CSV, ensures resource release)

options:
  -h, --help            show this help message and exit
  --runs RUNS           Number of runs per SQL file (default: 3)
  --duckdb-bin DUCKDB_BIN
                        Path to duckdb executable
  --sql-dir SQL_DIR     Directory containing SQL files (only processes .sql files starting with 'q')
  --output-csv OUTPUT_CSV
                        Path to output result CSV
  --wait-after-run WAIT_AFTER_RUN
                        Seconds to wait after each run (ensures resource release, default: 2s)
  --threads THREADS     Number of threads to use in DuckDB (default: 96)
  --benchmark BENCHMARK
                        Name of benchmark to use (must exist in benchmark JSON, e.g. clickbench-
                        pixels-e0)
  --benchmark-json BENCHMARK_JSON
                        Path to benchmark configuration JSON file (default: ./benchmark.json)

```
## 

## I/O粒度测试
`blk_stat.py`在执行`process_sqls.py`的同时,调用blktrace和blkprase读取底层块设备的I/O粒度，同时也需要注意运行的查询由`process_sql.py`内置 

## 单/双buffer性能测试
`single_doublebuffer_async_sync_test.py` 设置运行参数，执行单双buffer测试

## perf实验


## pixels-cli 

测试`pixels-cli`的正确性和功能性，生成文件之后，需要调用duckdb读取，得到正确结果

## bufferpool Size

验证bufferpool size对查询性能的影响，主要目的是，证明提前分配内存空间这个过程很耗时，需要尽可能减少内存分配，使用提前注册好的buffer
方法，使用q06 改变buffer大小，查看总内存消耗量，性能变化和page-fault触发的次数，以及cpu时间