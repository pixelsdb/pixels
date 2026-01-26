# Buffer Size 测试脚本

## 概述

这个脚本用于测试不同 `pixel.bufferpool.bufferpoolSize` 参数值对查询性能的影响。

## 功能

- 自动修改 `~/opt/pixels/etc/pixels-cpp.properties` 中的 `pixel.bufferpool.bufferpoolSize` 参数
- 使用 `./build/release/duckdb` 执行 `test-q06-24ssd.sql` 查询
- 调用 `run_perf.py` 来收集性能数据和生成火焰图
- 统计每个缓冲区大小配置下的查询执行时间
- 收集 page-faults（页错误）、minor-faults（次要页错误）、major-faults（主要页错误）统计信息
- 将所有收集的数据导出为 CSV 格式，方便后续分析

## 测试参数

- **参数范围**: 0 到 100MB (100 * 1024 * 1024 bytes)
- **步长**: 10MB (10 * 1024 * 1024 bytes)
- **测试值**: 0MB, 10MB, 20MB, 30MB, ..., 100MB

## 使用方法

### 前提条件

1. 确保 DuckDB 已编译: `./build/release/duckdb` 存在
2. 确保 `run_perf.py` 脚本在 `testcase/` 目录下
3. 确保 `generate_flamegraphs.sh` 脚本存在且可执行
4. 确保有 sudo 权限运行 perf 命令

### 运行脚本

```bash
cd /home/whz/test/pixels/cpp/testcase/buffersize-tests
python3 run_buffer_test.py
```

或者直接执行:

```bash
./run_buffer_test.py
```

## 输出

### 结果目录结构

```
testcase/buffersize-tests/results_buffer_test/
├── size_0/
│   ├── q06-24ssd-threads24-context-stat.txt
│   └── q06-24ssd-threads24-sched.svg
├── size_10485760/
│   ├── q06-24ssd-threads24-context-stat.txt
│   └── q06-24ssd-threads24-sched.svg
├── size_20971520/
│   ├── q06-24ssd-threads24-context-stat.txt
│   └── q06-24ssd-threads24-sched.svg
...
```

### 输出文件说明

- **context-stat.txt**: perf stat 统计信息，包含执行时间和各种性能计数器
- **sched.svg**: CPU 火焰图，用于分析性能瓶颈

### 控制台输出

脚本运行完成后会在控制台输出汇总表格:

```
============================================================
Buffer Size Test Summary
============================================================
Buffer Size (MB)     | Time (s)       
----------------------------------------
0.0                  | 12.3456        
10.0                 | 11.2345        
20.0                 | 10.5678        
...
============================================================
```

## 配置修改

如需修改测试参数，编辑 `run_buffer_test.py` 中的以下常量:

```python
# 修改最大缓冲区大小 (默认 100MB)
MAX_SIZE = 100 * 1024 * 1024

# 修改步长 (默认 10MB)
STEP_SIZE = 10 * 1024 * 1024

# 修改线程数 (默认 24)
THREADS = [24]

# 修改查询 (默认 q06)
QUERY = "q06"

# 修改 SSD 模式 (默认 24ssd)
SSD_MODE = "24ssd"
```

## 注意事项

1. 脚本会修改系统配置文件 `~/opt/pixels/etc/pixels-cpp.properties`
2. 每次测试后该配置文件会保留最后一次测试的参数值
3. 使用 perf 需要 sudo 权限
4. 测试过程可能需要较长时间，请耐心等待
5. 确保系统有足够的磁盘空间存储结果文件和火焰图
