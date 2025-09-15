import os
import re
import subprocess
import csv
import time
import psutil  # 新增：用于检查进程是否残留
from typing import List
import argparse

# -------------------------- 1. 配置参数（可通过命令行覆盖） --------------------------
def parse_args():
    parser = argparse.ArgumentParser(description="DuckDB ClickBench 批量测试脚本（多列CSV版，确保资源释放）")
    parser.add_argument(
        "--runs",
        type=int,
        default=3,
        help="每个SQL文件的运行次数（默认：3次）"
    )
    parser.add_argument(
        "--duckdb-bin",
        type=str,
        default="/home/whz/test/pixels/cpp/build/release/duckdb",
        help="duckdb可执行文件路径"
    )
    parser.add_argument(
        "--sql-dir",
        type=str,
        default="/home/whz/test/pixels/cpp/pixels-duckdb/duckdb/benchmark/clickbench/queries",
        help="SQL文件所在目录（仅处理以q开头的.sql文件）"
    )
    parser.add_argument(
        "--output-csv",
        type=str,
        default="/home/whz/test/pixels/cpp/duckdb_benchmark_result.csv",
        help="输出结果CSV路径"
    )
    parser.add_argument(
        "--wait-after-run",
        type=float,
        default=2.0,
        help="每次运行后等待的秒数（确保资源释放，默认2秒）"
    )
    return parser.parse_args()

# 创建 hits 视图的SQL（不变）
CREATE_VIEW_SQL = """
CREATE VIEW hits AS SELECT * FROM pixels_scan([
    '/data/9a3-01/clickbench/pixels-e0/hits/v-0-ordered/*',
    '/data/9a3-02/clickbench/pixels-e0/hits/v-0-ordered/*',
    '/data/9a3-03/clickbench/pixels-e0/hits/v-0-ordered/*',
    '/data/9a3-04/clickbench/pixels-e0/hits/v-0-ordered/*',
    '/data/9a3-05/clickbench/pixels-e0/hits/v-0-ordered/*',
    '/data/9a3-06/clickbench/pixels-e0/hits/v-0-ordered/*',
    '/data/9a3-07/clickbench/pixels-e0/hits/v-0-ordered/*',
    '/data/9a3-08/clickbench/pixels-e0/hits/v-0-ordered/*',
    '/data/9a3-09/clickbench/pixels-e0/hits/v-0-ordered/*',
    '/data/9a3-10/clickbench/pixels-e0/hits/v-0-ordered/*',
    '/data/9a3-11/clickbench/pixels-e0/hits/v-0-ordered/*',
    '/data/9a3-12/clickbench/pixels-e0/hits/v-0-ordered/*',
    '/data/9a3-13/clickbench/pixels-e0/hits/v-0-ordered/*',
    '/data/9a3-14/clickbench/pixels-e0/hits/v-0-ordered/*',
    '/data/9a3-15/clickbench/pixels-e0/hits/v-0-ordered/*',
    '/data/9a3-16/clickbench/pixels-e0/hits/v-0-ordered/*',
    '/data/9a3-17/clickbench/pixels-e0/hits/v-0-ordered/*',
    '/data/9a3-18/clickbench/pixels-e0/hits/v-0-ordered/*',
    '/data/9a3-19/clickbench/pixels-e0/hits/v-0-ordered/*',
    '/data/9a3-20/clickbench/pixels-e0/hits/v-0-ordered/*',
    '/data/9a3-21/clickbench/pixels-e0/hits/v-0-ordered/*',
    '/data/9a3-22/clickbench/pixels-e0/hits/v-0-ordered/*',
    '/data/9a3-23/clickbench/pixels-e0/hits/v-0-ordered/*',
    '/data/9a3-24/clickbench/pixels-e0/hits/v-0-ordered/*'
]);
"""

# -------------------------- 2. 核心工具函数（新增资源释放检查） --------------------------
def get_sql_files(sql_dir: str) -> List[str]:
    sql_files = []
    for filename in os.listdir(sql_dir):
        if filename.endswith(".sql") and filename.startswith("q"):
            sql_files.append(os.path.join(sql_dir, filename))
    sql_files.sort()
    if not sql_files:
        raise ValueError(f"SQL目录 {sql_dir} 下未找到以q开头的.sql文件！")
    return sql_files

def extract_real_time(duckdb_output: str) -> float:
    pattern = r"Run Time \(s\): real (\d+\.\d+)"
    match = re.search(pattern, duckdb_output, re.MULTILINE)
    if not match:
        raise ValueError(f"未提取到real时间！部分输出：\n{duckdb_output[:500]}...")
    return round(float(match.group(1)), 3)

def kill_remaining_duckdb(duckdb_bin: str):
    """检查并杀死残留的duckdb进程（防止资源占用）"""
    duckdb_name = os.path.basename(duckdb_bin)  # 获取duckdb进程名（如"duckdb"）
    for proc in psutil.process_iter(['name', 'cmdline']):
        try:
            # 匹配进程名或命令行中包含duckdb路径的进程
            if (proc.info['name'] == duckdb_name) or (duckdb_bin in ' '.join(proc.info['cmdline'] or [])):
                print(f"⚠️ 发现残留的{duckdb_name}进程（PID: {proc.pid}），正在杀死...")
                proc.terminate()  # 尝试优雅终止
                # 等待1秒，若未终止则强制杀死
                try:
                    proc.wait(timeout=1)
                except psutil.TimeoutExpired:
                    proc.kill()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

def run_single_sql(duckdb_bin: str, sql_content: str, wait_after_run: float) -> float:
    """单次运行SQL，确保进程退出并释放资源后再返回（兼容Python 2/3）"""
    duckdb_commands = f"{CREATE_VIEW_SQL.strip()}\nset threads=48;\n\n.timer on\n{sql_content.strip()}\n.exit"
    process = None  # 定义进程变量，便于异常处理中关闭

    try:
        # 关键修改：用 stdin=subprocess.PIPE 替代 input 参数（Python 2/3 通用）
        process = subprocess.Popen(
            [duckdb_bin],
            stdin=subprocess.PIPE,  # 开启标准输入管道，用于传递SQL命令
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT
        )

        # 关键修改：处理Python 2/3的编码差异（Python 2传字符串，Python 3传字节流）
        if isinstance(duckdb_commands, str):
            # Python 3：将字符串编码为字节流
            input_data = duckdb_commands.encode("utf-8")
        else:
            # Python 2：直接使用字符串（默认ASCII，若有中文需调整）
            input_data = duckdb_commands

        # 传递输入并等待进程完成（communicate() 会自动处理 stdin 写入和 stdout 读取）
        stdout, _ = process.communicate(input=input_data, timeout=360)  #

        # 处理输出编码（Python 3 stdout是字节流，需解码；Python 2是字符串）
        if isinstance(stdout, bytes):
            output = stdout.decode("utf-8", errors="ignore")
        else:
            output = stdout

        # 检查退出码（非0表示执行失败）
        if process.returncode != 0:
            raise RuntimeError(
                f"duckdb执行失败（返回码{process.returncode}）：\n{output[:1000]}..."
            )

        # 提取时间并返回
        real_time = extract_real_time(output)

        # 强制等待进程资源释放
        time.sleep(wait_after_run)

        # 检查是否有残留进程
        kill_remaining_duckdb(duckdb_bin)

        return real_time

    except subprocess.TimeoutExpired:
        if process:
            process.kill()  # 超时则强制杀死进程
        raise RuntimeError("duckdb执行超时（超过1小时）") from None
    finally:
        # 最终确保进程已退出
        if process and process.poll() is None:
            process.kill()
            print("⚠️ 强制终止未退出的duckdb进程")
def init_csv(output_csv: str, runs: int):
    headers = ["SQL文件名"] + [f"运行{idx}时间(秒)" for idx in range(1, runs + 1)]
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
    print(f"✅ 初始化多列CSV成功，表头：{','.join(headers)}")

def write_single_row(output_csv: str, sql_filename: str, run_times: List[float], runs: int):
    row_data = {"SQL文件名": sql_filename}
    for idx in range(1, runs + 1):
        time_val = run_times[idx - 1] if (idx - 1) < len(run_times) else ""
        row_data[f"运行{idx}时间(秒)"] = time_val
    with open(output_csv, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=row_data.keys())
        writer.writerow(row_data)

# -------------------------- 3. 主逻辑 --------------------------
def main():
    args = parse_args()
    print("=" * 70)
    print("DuckDB ClickBench 批量测试脚本（确保资源释放版）")
    print(f"配置：每个SQL运行{args.runs}次，每次运行后等待{args.wait_after_run}秒")
    print(f"duckdb路径：{args.duckdb_bin}")
    print(f"SQL目录：{args.sql_dir}")
    print(f"输出CSV：{args.output_csv}")
    print("=" * 70)

    # 初始化前先检查并杀死可能残留的duckdb进程
    kill_remaining_duckdb(args.duckdb_bin)

    # 初始化CSV
    init_csv(args.output_csv, args.runs)

    # 获取SQL文件
    try:
        sql_files = get_sql_files(args.sql_dir)
        print(f"\n✅ 找到 {len(sql_files)} 个符合条件的SQL文件：")
        for i, f in enumerate(sql_files, 1):
            print(f"   {i:2d}. {os.path.basename(f)}")
    except ValueError as e:
        print(f"\n❌ 错误：{e}")
        return

    # 逐个运行SQL文件
    for sql_file in sql_files:
        sql_filename = os.path.basename(sql_file).replace(".sql", "")
        # print(sql_filename)
        # if sql_filename=="q24":
        print(f"\n{'=' * 60}")
        print(f"开始处理：{sql_filename}.sql")
        print(f"{'=' * 60}")

        # 读取SQL内容
        try:
            with open(sql_file, "r", encoding="utf-8") as f:
                sql_content = f.read()
            print(f"✅ 成功读取SQL文件（内容长度：{len(sql_content)} 字符）")
        except Exception as e:
            print(f"❌ 读取SQL文件失败：{e}")
            write_single_row(args.output_csv, sql_filename, [], args.runs)
            continue

        # 多次运行并记录时间
        run_times = []
        for run_idx in range(1, args.runs + 1):
            print(f"\n--- 第 {run_idx:2d}/{args.runs} 次运行 ---")
            try:
                # 调用优化后的run_single_sql，确保资源释放
                real_time = run_single_sql(args.duckdb_bin, sql_content, args.wait_after_run)
                run_times.append(real_time)
                print(f"✅ 运行成功，用时：{real_time} 秒")
            except (RuntimeError, ValueError) as e:
                print(f"❌ 运行失败：{e}")
                continue

        # 写入CSV
        write_single_row(args.output_csv, sql_filename, run_times, args.runs)
        print(f"\n✅ 已写入CSV：{sql_filename}.sql → 有效运行次数：{len(run_times)}/{args.runs}")

    # 全部完成后再次检查残留进程
    kill_remaining_duckdb(args.duckdb_bin)
    print(f"\n{'=' * 70}")
    print("所有SQL文件处理完成！")
    print(f"多列格式CSV：{args.output_csv}")
    print("=" * 70)

if __name__ == "__main__":
    main()