import os
import re
import subprocess
import csv
import time
import psutil  # For checking residual processes
from typing import List
import argparse

# -------------------------- 1. Configuration Parameters (overridable via CLI) --------------------------
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
        default="/home/whz/test/pixels/cpp/pixels-duckdb/duckdb/benchmark/clickbench/queries",
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
    return parser.parse_args()

# SQL to create hits view (unchanged)
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

# -------------------------- 2. Core Utility Functions (added resource release checks) --------------------------
def get_sql_files(sql_dir: str) -> List[str]:
    """Get sorted list of SQL files starting with 'q' in target directory"""
    sql_files = []
    for filename in os.listdir(sql_dir):
        if filename.endswith(".sql") and filename.startswith("q"):
            sql_files.append(os.path.join(sql_dir, filename))
    sql_files.sort()
    if not sql_files:
        raise ValueError(f"No .sql files starting with 'q' found in {sql_dir}!")
    return sql_files

def extract_real_time(duckdb_output: str) -> float:
    """Extract real execution time (in seconds) from duckdb output"""
    pattern = r"Run Time \(s\): real (\d+\.\d+)"
    match = re.search(pattern, duckdb_output, re.MULTILINE)
    if not match:
        raise ValueError(f"Failed to extract real time! Partial output:\n{duckdb_output[:500]}...")
    return round(float(match.group(1)), 3)

def kill_remaining_duckdb(duckdb_bin: str):
    """Check and kill residual duckdb processes to prevent resource leaks"""
    duckdb_name = os.path.basename(duckdb_bin)
    for proc in psutil.process_iter(['name', 'cmdline']):
        try:
            # Match processes by name or command line containing duckdb path
            if (proc.info['name'] == duckdb_name) or (duckdb_bin in ' '.join(proc.info['cmdline'] or [])):
                print(f"⚠️ Found residual {duckdb_name} process (PID: {proc.pid}), killing...")
                proc.terminate()
                # Force kill if not terminated within 1 second
                try:
                    proc.wait(timeout=1)
                except psutil.TimeoutExpired:
                    proc.kill()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

def run_single_sql(duckdb_bin: str, sql_content: str, wait_after_run: float) -> float:
    """Run SQL once, ensure process exits and resources are released before returning"""
    duckdb_commands = f"{CREATE_VIEW_SQL.strip()}\nset threads=48;\n\n.timer on\n{sql_content.strip()}\n.exit"
    process = None  # Initialize process variable for exception handling

    try:
        # Use stdin pipe for Python 2/3 compatibility (instead of input parameter)
        process = subprocess.Popen(
            [duckdb_bin],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT
        )

        # Handle encoding differences between Python 2/3
        if isinstance(duckdb_commands, str):
            input_data = duckdb_commands.encode("utf-8")  # Python 3: string to bytes
        else:
            input_data = duckdb_commands  # Python 2: use string directly

        # Pass input and wait for completion (timeout after 1 hour)
        stdout, _ = process.communicate(input=input_data, timeout=360)

        # Decode output (Python 3 returns bytes, Python 2 returns string)
        if isinstance(stdout, bytes):
            output = stdout.decode("utf-8", errors="ignore")
        else:
            output = stdout

        # Check exit code
        if process.returncode != 0:
            raise RuntimeError(
                f"duckdb execution failed (code {process.returncode}):\n{output[:1000]}..."
            )

        # Extract and return execution time
        real_time = extract_real_time(output)

        # Wait to ensure resource release
        time.sleep(wait_after_run)

        # Final check for residual processes
        kill_remaining_duckdb(duckdb_bin)

        return real_time

    except subprocess.TimeoutExpired:
        if process:
            process.kill()
        raise RuntimeError("duckdb execution timed out (exceeded 1 hour)") from None
    finally:
        # Ensure process is terminated
        if process and process.poll() is None:
            process.kill()
            print("⚠️ Forcibly terminated non-exiting duckdb process")

def init_csv(output_csv: str, runs: int):
    """Initialize CSV file with headers"""
    headers = ["SQL File Name"] + [f"Run {idx} Time (s)" for idx in range(1, runs + 1)]
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
    print(f"✅ Initialized multi-column CSV with headers: {','.join(headers)}")

def write_single_row(output_csv: str, sql_filename: str, run_times: List[float], runs: int):
    """Write single SQL file's results to CSV"""
    row_data = {"SQL File Name": sql_filename}
    for idx in range(1, runs + 1):
        time_val = run_times[idx - 1] if (idx - 1) < len(run_times) else ""
        row_data[f"Run {idx} Time (s)"] = time_val
    with open(output_csv, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=row_data.keys())
        writer.writerow(row_data)

# -------------------------- 3. Main Logic --------------------------
def main():
    args = parse_args()
    print("=" * 70)
    print("DuckDB ClickBench Batch Test Script (Resource Release Ensured)")
    print(f"Config: {args.runs} runs per SQL, {args.wait_after_run}s wait after each run")
    print(f"DuckDB path: {args.duckdb_bin}")
    print(f"SQL directory: {args.sql_dir}")
    print(f"Output CSV: {args.output_csv}")
    print("=" * 70)

    # Kill residual duckdb processes before starting
    kill_remaining_duckdb(args.duckdb_bin)

    # Initialize result CSV
    init_csv(args.output_csv, args.runs)

    # Get list of SQL files
    try:
        sql_files = get_sql_files(args.sql_dir)
        print(f"\n✅ Found {len(sql_files)} eligible SQL files:")
        for i, f in enumerate(sql_files, 1):
            print(f"   {i:2d}. {os.path.basename(f)}")
    except ValueError as e:
        print(f"\n❌ Error: {e}")
        return

    # Process each SQL file
    for sql_file in sql_files:
        sql_filename = os.path.basename(sql_file).replace(".sql", "")
        print(f"\n{'=' * 60}")
        print(f"Processing: {sql_filename}.sql")
        print(f"{'=' * 60}")

        # Read SQL content
        try:
            with open(sql_file, "r", encoding="utf-8") as f:
                sql_content = f.read()
            print(f"✅ Successfully read SQL file (content length: {len(sql_content)} chars)")
        except Exception as e:
            print(f"❌ Failed to read SQL file: {e}")
            write_single_row(args.output_csv, sql_filename, [], args.runs)
            continue

        # Run multiple times and record results
        run_times = []
        for run_idx in range(1, args.runs + 1):
            print(f"\n--- Run {run_idx:2d}/{args.runs} ---")
            try:
                real_time = run_single_sql(args.duckdb_bin, sql_content, args.wait_after_run)
                run_times.append(real_time)
                print(f"✅ Run successful, time: {real_time}s")
            except (RuntimeError, ValueError) as e:
                print(f"❌ Run failed: {e}")
                continue

        # Write results to CSV
        write_single_row(args.output_csv, sql_filename, run_times, args.runs)
        print(f"\n✅ Written to CSV: {sql_filename}.sql → Valid runs: {len(run_times)}/{args.runs}")

    # Final check for residual processes after all tests
    kill_remaining_duckdb(args.duckdb_bin)
    print(f"\n{'=' * 70}")
    print("All SQL files processed!")
    print(f"Multi-column CSV: {args.output_csv}")
    print("=" * 70)

if __name__ == "__main__":
    main()