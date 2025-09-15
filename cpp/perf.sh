#!/bin/bash

# 检查参数数量
if [ $# -ne 2 ]; then
    echo "用法: $0 <查询文件> <输出文件前缀>"
    echo "示例: $0 test_q01.sql query_1"
    exit 1
fi

# 赋值参数
QUERY_FILE="$1"
OUTPUT_PREFIX="$2"

# 记录开始时间
START_TIME=$(date +%s%N)
PREVIOUS_TIME=$START_TIME

# 显示时间的函数
show_time() {
    local start=$1
    local end=$2
    local stage=$3

    # 计算毫秒数（bash中整数运算）
    local duration=$(( (end - start) / 1000000 ))
    echo "阶段 '$stage' 耗时: ${duration}ms"
}

echo "开始执行性能分析..."

# 运行perf记录
echo "1. 运行perf记录..."
sudo -E perf record -F 1 --call-graph=dwarf -g ./build/release/duckdb < "$QUERY_FILE"
CURRENT_TIME=$(date +%s%N)
show_time $PREVIOUS_TIME $CURRENT_TIME "运行perf record"
PREVIOUS_TIME=$CURRENT_TIME

# 生成perf脚本输出
echo "2. 生成perf脚本输出..."
sudo perf script -i perf.data > "${OUTPUT_PREFIX}.perf"
CURRENT_TIME=$(date +%s%N)
show_time $PREVIOUS_TIME $CURRENT_TIME "生成perf脚本输出"
PREVIOUS_TIME=$CURRENT_TIME

# 折叠调用栈
echo "3. 折叠调用栈..."
stackcollapse-perf.pl "${OUTPUT_PREFIX}.perf" > "${OUTPUT_PREFIX}.folded"
CURRENT_TIME=$(date +%s%N)
show_time $PREVIOUS_TIME $CURRENT_TIME "折叠调用栈"
PREVIOUS_TIME=$CURRENT_TIME

# 生成火焰图
echo "4. 生成火焰图..."
flamegraph.pl "${OUTPUT_PREFIX}.folded" > "${OUTPUT_PREFIX}-cpu.svg"
CURRENT_TIME=$(date +%s%N)
show_time $PREVIOUS_TIME $CURRENT_TIME "生成火焰图"
PREVIOUS_TIME=$CURRENT_TIME

# 重命名perf数据文件
echo "5. 重命名perf数据文件..."
mv perf.data "${OUTPUT_PREFIX}-perf.data"
CURRENT_TIME=$(date +%s%N)
show_time $PREVIOUS_TIME $CURRENT_TIME "重命名文件"
PREVIOUS_TIME=$CURRENT_TIME

# 计算总时间
TOTAL_DURATION=$(( (CURRENT_TIME - START_TIME) / 1000000 ))
echo "总耗时: ${TOTAL_DURATION}ms"

echo "操作完成，生成的文件："
echo "${OUTPUT_PREFIX}.perf"
echo "${OUTPUT_PREFIX}.folded"
echo "${OUTPUT_PREFIX}-cpu.svg"
echo "${OUTPUT_PREFIX}-perf.data"
