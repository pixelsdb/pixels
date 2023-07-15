# pixels experiment

## Optimization 1: change big endian to small endian

In the original pixels version, the pixels data(integer, long, date and string) is stored as big endian. However, in c++ version, we read data as small endian. 

For example, if we define int a = 5, the small endian is: 
```
05 00 00 00
```
While the large endian is:
```
00 00 00 05
```

Therefore, c++ needs to transform large endian to small endian. Here we have two drawbacks: a. transforming large endian to small endian takes overheads; b. In the case of disabling pixels encoding, we aim to send the pointer of ByteBuffer to Duckdb directly. However, the large endian data makes it impossible since we have to read ByteBuffer to another memory and change the data to small endian one by one. 

Therefore, in order to send the pointer of ByteBuffer to Duckdb directly, we do two optimizations here:

* change large endian to small endian in the pixels writer
* remove the isLong indicator in the pixels IntegerColumnWriter



## Optimization 2: reuse ByteBuffer

### Description
In the original pixels version, we allocate a new ByteBuffer for each column of each file. If the table is very large, we might create too many ByteBuffer. Allocating new memory has overheads, and it is hard to manage the memory.

In this optimization, we reuse ByteBuffer when we process the next file. For example, now we have a 200GB lineitem table with 1800 pixel files, and we read 4 lineitem columns. In the previous implementation, we need to allocate `4 * 1800 = 7200` ByteBuffers. In the current optimization, we only need to allocate `4 * #thread` ByteBuffers. Each thread creates its own ByteBuffer. 

Here are three advantages: 

* Decrease the overhead of allocating memory

* Later we aim to use iouring/aio to replace pread. iouring needs to register buffer to kernel space. Registering too many ByteBuffer would cause a lot of overheads. 

* We found that the result of previous version is not stable somehow. For example, in the tpch query 06, if page cache is enabled, sometimes it takes 6s while sometimes it takes only 3s. Based on the vtune result, `pread` sometimes takes a long time while other times it takes less time. We noticed that the optimization version has more stable result for q06 (3.0-4.0s).


### Experiment

We run the benchmark in the following command:

pixels v1:
```
cd /scratch/liyu/opt/duckdb/extension/pixels/pixels-reader-cxx
git checkout pushdown-duckdb
cd /scratch/liyu/opt/duckdb
git checkout pushdown-duckdb
BUILD_BENCHMARK=1 BUILD_TPCH=1 make -j
python scripts/run_benchmark.py --pixels "benchmark/tpch/pixels/tpch_300_small_endian/" --from-page-cache -v --repeat-time-disk 1 --repeat-time-page-cache 1
```


pixels v1 with encoding:
```
cd /scratch/liyu/opt/duckdb/extension/pixels/pixels-reader-cxx
git checkout pushdown-duckdb
cd /scratch/liyu/opt/duckdb
git checkout pushdown-duckdb
BUILD_BENCHMARK=1 BUILD_TPCH=1 make -j
python scripts/run_benchmark.py --pixels "benchmark/tpch/pixels/tpch_300_encoding_small_endian/" --from-page-cache -v --repeat-time-disk 1 --repeat-time-page-cache 1
```

pixels v2:
```
cd /scratch/liyu/opt/duckdb/extension/pixels/pixels-reader-cxx
git checkout buffer-pool
cd /scratch/liyu/opt/duckdb
git checkout buffer-pool
BUILD_BENCHMARK=1 BUILD_TPCH=1 make -j
python scripts/run_benchmark.py --pixels "benchmark/tpch/pixels/tpch_300_small_endian/" --from-page-cache -v --repeat-time-disk 1 --repeat-time-page-cache 1
```

pixels v2 with encoding:
```
cd /scratch/liyu/opt/duckdb/extension/pixels/pixels-reader-cxx
git checkout buffer-pool
cd /scratch/liyu/opt/duckdb
git checkout buffer-pool
BUILD_BENCHMARK=1 BUILD_TPCH=1 make -j
python scripts/run_benchmark.py --pixels "benchmark/tpch/pixels/tpch_300_encoding_small_endian/" --from-page-cache -v --repeat-time-disk 1 --repeat-time-page-cache 1
```

pixels v1 vs v2 tpch:

<img src="plot/pixels-tpch-v1-v2.png">

pixels v1 vs v2 tpch with encoding:

<img src="plot/pixels-encoding-tpch-v1-v2.png">

We also test our result in a micro-benchmark:

pixels v1 vs v2 micro benchmark:

<img src="plot/pixels-micro-v1-v2.png">

pixels v1 vs v2 micro benchmark with encoding:

<img src="plot/pixels-encoding-micro-v1-v2.png">

## Optimization 3: iouring

We use iouring to replace pread. iouring is an async io library. It has its specific optimization on IO and we expect it is faster than pread.

### Code design

For each table, we would create `#threads` threads to execute it. We create `#threads` iouring instances to proceed it instead of creating iouring instance for each pxl file. In this way, the initialization and exit cost would be decreased, which means that `ring` is thread local.

### performance

<img src="plot/parquet-vs-pixels.png">

pixels-v2 is reuse buffer optimization (discussed above).