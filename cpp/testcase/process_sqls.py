import os
import re
import subprocess
import csv
import time
import psutil
import json  # Added: For parsing benchmark configuration files
from typing import List
import argparse

# -------------------------- 1. Basic Configuration (Added default benchmark JSON path) --------------------------
# Default path to benchmark configuration file (can be overridden via CLI parameter)
DEFAULT_BENCHMARK_JSON = "./benchmark.json"


def clear_page_cache():
    """Clear Linux page cache to ensure fair benchmarking"""
    try:
        print("🧹 Clearing Linux page cache...")
        subprocess.run(["sync"], check=True)
        subprocess.run(["sudo", "bash", "-c", "echo 3 > /proc/sys/vm/drop_caches"], check=True)
        print("✅ Page cache cleared successfully")
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Failed to clear page cache: {e}")


# -------------------------- 2. CLI Argument Parsing (Added benchmark-related parameters) --------------------------
def parse_args():
    parser = argparse.ArgumentParser(description="DuckDB ClickBench Batch Test Script (Multi-column CSV, ensures resource release)")
    parser.add_argument(
        "--runs",
        type=int,
        default=3,
        help="Number of runs per SQL file (default: 3)"
    )
    parser.add_argument(
        "--duckdb-bin",
        type=str,
        default="/home/whz/test/pixels/cpp/build/release/duckdb",
        help="Path to duckdb executable"
    )
    parser.add_argument(
        "--sql-dir",
        type=str,
        default="/home/whz/test/pixels/cpp/pixels-duckdb/duckdb/benchmark/clickbench/queries-test",
        help="Directory containing SQL files (only processes .sql files starting with 'q')"
    )
    parser.add_argument(
        "--output-csv",
        type=str,
        default="/home/whz/test/pixels/cpp/duckdb_benchmark_result.csv",
        help="Path to output result CSV"
    )
    parser.add_argument(
        "--wait-after-run",
        type=float,
        default=2.0,
        help="Seconds to wait after each run (ensures resource release, default: 2s)"
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=96,
        help="Number of threads to use in DuckDB (default: 96)"
    )
    parser.add_argument(
        "--benchmark",
        type=str,
        default="clickbench-pixels-e0-1ssd",
        help="Name of benchmark to use (must exist in benchmark JSON, e.g. clickbench-pixels-e0)"
    )
    parser.add_argument(
        "--benchmark-json",
        type=str,
        default=DEFAULT_BENCHMARK_JSON,
        help=f"Path to benchmark configuration JSON file (default: {DEFAULT_BENCHMARK_JSON})"
    )
    return parser.parse_args()


# -------------------------- 3. Core Utility Functions --------------------------
def get_sql_files(sql_dir: str) -> List[str]:
    sql_files = []
    for filename in os.listdir(sql_dir):
        if filename.endswith(".sql") and filename.startswith("q"):
            sql_files.append(os.path.join(sql_dir, filename))
    sql_files.sort()
    if not sql_files:
        raise ValueError(f"No .sql files starting with 'q' found in {sql_dir}!")
    return sql_files


def extract_real_time(duckdb_output: str) -> float:
    pattern = r"Run Time \(s\): real (\d+\.\d+)"
    match = re.search(pattern, duckdb_output, re.MULTILINE)
    if not match:
        raise ValueError(f"Failed to extract real time! Partial output:\n{duckdb_output[:500]}...")
    return round(float(match.group(1)), 3)


def kill_remaining_duckdb(duckdb_bin: str):
    duckdb_name = os.path.basename(duckdb_bin)
    for proc in psutil.process_iter(['name', 'cmdline']):
        try:
            if (proc.info['name'] == duckdb_name) or (duckdb_bin in ' '.join(proc.info['cmdline'] or [])):
                print(f"⚠️ Found residual {duckdb_name} process (PID: {proc.pid}), killing...")
                proc.terminate()
                try:
                    proc.wait(timeout=1)
                except psutil.TimeoutExpired:
                    proc.kill()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue


def load_benchmark_create_view(benchmark_json_path: str, benchmark_name: str) -> str:
    if not os.path.exists(benchmark_json_path):
        raise FileNotFoundError(f"Benchmark JSON file not found: {benchmark_json_path}")

    with open(benchmark_json_path, "r", encoding="utf-8") as f:
        try:
            benchmark_config = json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"Failed to parse benchmark JSON: {str(e)}")

    if benchmark_name not in benchmark_config:
        available_benchmarks = ", ".join(benchmark_config.keys())
        raise KeyError(f"Benchmark '{benchmark_name}' not found. Available benchmarks: {available_benchmarks}")

    create_view_sql = benchmark_config[benchmark_name].strip()
    if not create_view_sql:
        raise ValueError(f"CREATE VIEW SQL for benchmark '{benchmark_name}' is empty in JSON")

    return create_view_sql


def run_single_sql(duckdb_bin: str, create_view_sql: str, sql_content: str, wait_after_run: float, threads: int) -> float:
    duckdb_commands = f"{create_view_sql}\nset threads={threads};\n\n.timer on\nexplain analyze {sql_content.strip()}\n.exit"
    process = None

    try:
        process = subprocess.Popen(
            [duckdb_bin],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT
        )

        input_data = duckdb_commands.encode("utf-8")
        stdout, _ = process.communicate(input=input_data, timeout=3600)

        output = stdout.decode("utf-8", errors="ignore")

        if process.returncode != 0:
            raise RuntimeError(f"duckdb execution failed (code {process.returncode}):\n{output[:1000]}...")
        print(output)
        real_time = extract_real_time(output)
        time.sleep(wait_after_run)
        kill_remaining_duckdb(duckdb_bin)
        return real_time

    except subprocess.TimeoutExpired:
        if process:
            process.kill()
        raise RuntimeError("duckdb execution timed out (exceeded 1 hour)") from None
    finally:
        if process and process.poll() is None:
            process.kill()
            print("⚠️ Forcibly terminated non-exiting duckdb process")


def init_csv(output_csv: str, runs: int):
    headers = ["SQL File Name"] + [f"Run {idx} Time (s)" for idx in range(1, runs + 1)]
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
    print(f"✅ Initialized multi-column CSV with headers: {','.join(headers)}")


def write_single_row(output_csv: str, sql_filename: str, run_times: List[float], runs: int):
    row_data = {"SQL File Name": sql_filename}
    for idx in range(1, runs + 1):
        time_val = run_times[idx - 1] if (idx - 1) < len(run_times) else ""
        row_data[f"Run {idx} Time (s)"] = time_val
    with open(output_csv, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=row_data.keys())
        writer.writerow(row_data)


# -------------------------- 4. Main Logic --------------------------
def main():
    args = parse_args()
    print("=" * 70)
    print("DuckDB ClickBench Batch Test Script (Resource Release Ensured)")
    print(f"Config: {args.runs} runs per SQL, {args.wait_after_run}s wait after each run")
    print(f"Benchmark: {args.benchmark} (from {args.benchmark_json})")
    print(f"DuckDB path: {args.duckdb_bin}")
    print(f"Threads: {args.threads}")
    print(f"SQL directory: {args.sql_dir}")
    print(f"Output CSV: {args.output_csv}")
    print("=" * 70)

    # clear_page_cache()

    kill_remaining_duckdb(args.duckdb_bin)
    try:
        create_view_sql = load_benchmark_create_view(args.benchmark_json, args.benchmark)
        print(f"✅ Loaded CREATE VIEW SQL for benchmark '{args.benchmark}'")
    except (FileNotFoundError, KeyError, ValueError) as e:
        print(f"\n❌ Benchmark initialization failed: {str(e)}")
        return

    init_csv(args.output_csv, args.runs)
    try:
        sql_files = get_sql_files(args.sql_dir)
        print(f"\n✅ Found {len(sql_files)} eligible SQL files:")
        for i, f in enumerate(sql_files, 1):
            print(f"   {i:2d}. {os.path.basename(f)}")
    except ValueError as e:
        print(f"\n❌ Error: {e}")
        return

    for sql_file in sql_files:
        sql_filename = os.path.basename(sql_file).replace(".sql", "")
        print(f"\n{'=' * 60}")
        print(f"Processing: {sql_filename}.sql")
        print(f"{'=' * 60}")

        try:
            with open(sql_file, "r", encoding="utf-8") as f:
                sql_content = f.read()
            print(f"✅ Successfully read SQL file (content length: {len(sql_content)} chars)")
        except Exception as e:
            print(f"❌ Failed to read SQL file: {e}")
            write_single_row(args.output_csv, sql_filename, [], args.runs)
            continue

        run_times = []
        for run_idx in range(1, args.runs + 1):
            print(f"\n--- Run {run_idx:2d}/{args.runs} ---")
            clear_page_cache()
            try:
                real_time = run_single_sql(args.duckdb_bin, create_view_sql, sql_content, args.wait_after_run, args.threads)
                run_times.append(real_time)
                print(f"✅ Run successful, time: {real_time}s")
            except (RuntimeError, ValueError) as e:
                print(f"❌ Run failed: {e}")
                continue

        write_single_row(args.output_csv, sql_filename, run_times, args.runs)
        print(f"\n✅ Written to CSV: {sql_filename}.sql → Valid runs: {len(run_times)}/{args.runs}")

    kill_remaining_duckdb(args.duckdb_bin)
    print(f"\n{'=' * 70}")
    print("All SQL files processed!")
    print(f"Multi-column CSV: {args.output_csv}")
    print("=" * 70)


if __name__ == "__main__":
    main()
