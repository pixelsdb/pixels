import subprocess
import os
import re
import argparse # Import argparse for command-line arguments

# --- Configuration (Defaults & Constants) ---
# Default lists (can be overridden by command-line arguments)
# THREADS = [1, 2, 4, 8, 16, 24, 32, 48, 64, 96]
# THREADS=[24]
# QUERIES = ["q01", "q24", "q33"]
# SSD_MODES = ["1ssd", "24ssd"]
# SSD_MODES=["1ssd"]
# THREADS = [32, 48]
# # THREADS = [24]
# # QUERIES = ["q01"]
# QUERIES = ["q01"]
# SSD_MODES = ["1ssd"]
# THREADS = [ 96]
# THREADS=[24]
# QUERIES = ["q24"]
# SSD_MODES = ["24ssd"]
THREADS = [24]
QUERIES = ["q01", "q24", "q33"]
SSD_MODES = ["1ssd"]

# Base 'perf stat' command: focusing on CPU and scheduling metrics
PERF_CMD_BASE = [
    "sudo", "-E", "perf", "stat",
    "-e", "cycles,instructions,cache-references,cache-misses,branches,branch-misses",
    "-e", "page-faults,minor-faults,major-faults",
    "-e", "task-clock,context-switches"
]

# Path to the DuckDB binary
DUCKDB_BINARY = "../build/release/duckdb"

# FlameGraph Bash script path (updated for scheduling events)
FLAMEGRAPH_SCRIPT = "./generate_flamegraphs.sh"


def ensure_result_dir(result_dir):
    """Create the results output directory"""
    if not os.path.exists(result_dir):
        os.makedirs(result_dir)
        print(f"Created directory: {result_dir}")


def update_sql_thread(sql_dir, sql_filename, thread_value, result_dir):
    """
    Replaces 'set threads=x;' in the SQL file with the specified value
    and writes the content to a temporary file in the result directory.
    """
    # Construct the full path to the base SQL file
    sql_path = os.path.join(sql_dir, sql_filename)

    with open(sql_path, "r") as f:
        content = f.read()

    # Replace or add set threads=x;
    # Try to replace existing one, if not found, assume it goes at the start
    if re.search(r"set\s+threads\s*=\s*\d+;", content):
        new_content = re.sub(r"set\s+threads\s*=\s*\d+;", f"set threads={thread_value};", content)
    else:
        # If no 'set threads' line is found, add it to the beginning
        new_content = f"set threads={thread_value};\n{content}"


    # Temporary file name uses the result directory
    tmp_path = os.path.join(result_dir, f"{os.path.basename(sql_filename)}.tmp_threads_{thread_value}.sql")
    with open(tmp_path, "w") as f:
        f.write(new_content)

    return tmp_path


def run_perf_stat_switches(query_file, query_name, ssd_mode, thread_value, result_dir):
    """
    Executes perf stat, collects context switch metrics, and outputs the result
    to a file in the results directory.
    """
    output_name = f"{query_name}-{ssd_mode}-threads{thread_value}-context-stat.txt"
    output_path = os.path.join(result_dir, output_name)

    # PERF_CMD_BASE is defined at the top of the script
    cmd = PERF_CMD_BASE + ["-o", output_path, DUCKDB_BINARY]

    print(f"\n--- 1. Running perf stat for context switches: {output_name} ---")

    try:
        with open(query_file, "r") as sql_f:
            subprocess.run(cmd, stdin=sql_f, check=True) # Use check=True to ensure command execution success
        print(f"==> perf stat output saved to: {output_path}")
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] perf stat failed for {output_name}: {e}")
        return False
    except FileNotFoundError:
        print(f"[ERROR] DuckDB binary not found at {DUCKDB_BINARY}")
        return False

    return True

def run_sched_flamegraph(tmp_sql_path, query_name, ssd_mode, thread_value, result_dir):
    """
    Calls an external Bash script to generate a flame graph focused on scheduling events.
    """
    if not os.path.exists(FLAMEGRAPH_SCRIPT):
        print(f"[WARN] Schedule FlameGraph script not found: {FLAMEGRAPH_SCRIPT}. Skipping analysis.")
        return

    print(f"\n--- 2. Running Schedule FlameGraph analysis ---")

    # Ensure the Bash script has execute permissions
    if not os.access(FLAMEGRAPH_SCRIPT, os.X_OK):
        print(f"[WARN] Adding execute permission to {FLAMEGRAPH_SCRIPT}")
        os.chmod(FLAMEGRAPH_SCRIPT, 0o755)

    output_html_name = f"{query_name}-{ssd_mode}-threads{thread_value}-sched.svg"
    output_html_path = os.path.join(result_dir, output_html_name)

    # Call the Bash script, passing the temporary SQL file, DuckDB binary path, and output HTML path
    flamegraph_cmd = [FLAMEGRAPH_SCRIPT, tmp_sql_path, DUCKDB_BINARY, output_html_path]

    try:
        subprocess.run(flamegraph_cmd, check=True)
        print(f"==> Schedule FlameGraph saved to: {output_html_path}")
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Schedule FlameGraph script failed for {tmp_sql_path}: {e}")
    except FileNotFoundError:
        print(f"[ERROR] FlameGraph script file not found at {FLAMEGRAPH_SCRIPT}")


def main():
    parser = argparse.ArgumentParser(description="Run DuckDB benchmarks with perf stat and FlameGraph analysis.")

    # --- New required arguments for directories ---
    parser.add_argument("--sql-dir", required=False, default="perf-pixels" ,help="Directory containing the base SQL query files (e.g., test-q01-1ssd.sql).")
    parser.add_argument("--result-dir", required=False,default="perf-pixels", help="Directory where temporary files and final results (perf stats, SVG) will be saved.")
    THREADS = [24]
    QUERIES = ["q01", "q24", "q33"]
    SSD_MODES = ["1ssd"]
    # --- Optional arguments with defaults ---
    parser.add_argument("--threads", type=int, nargs='+', default=THREADS, help=f"List of thread counts to test (default: {THREADS}).")
    parser.add_argument("--queries", nargs='+', default=QUERIES, help=f"List of query IDs to test (default: {QUERIES}).")
    parser.add_argument("--ssd-modes", nargs='+', default=SSD_MODES, help=f"List of SSD configurations (default: {SSD_MODES}).")

    args = parser.parse_args()

    # Assign parsed arguments to variables
    RESULT_DIR = args.result_dir
    SQL_DIR = args.sql_dir
    THREADS = args.threads
    QUERIES = args.queries
    SSD_MODES = args.ssd_modes

    # Ensure the result directory exists
    ensure_result_dir(RESULT_DIR)

    for q in QUERIES:
        for mode in SSD_MODES:

            sql_file_base = f"test-{q}-{mode}.sql"
            # Check for the file in the specified SQL_DIR
            sql_file_path_check = os.path.join(SQL_DIR, sql_file_base)

            if not os.path.exists(sql_file_path_check):
                print(f"[WARN] Base SQL file not found: {sql_file_path_check}, skipping")
                continue

            for t in THREADS:
                print("=" * 50)
                print(f"Starting analysis for Q={q}, Mode={mode}, Threads={t}")

                # 1. Update threads and create temporary SQL file in RESULT_DIR
                tmp_sql = update_sql_thread(
                    SQL_DIR, sql_file_base, t, RESULT_DIR
                )

                # 2. Run perf stat to collect context switch metrics
                success = run_perf_stat_switches(tmp_sql, q, mode, t, RESULT_DIR)

                # 3. If perf stat succeeded, run scheduling FlameGraph analysis
                if success:
                    run_sched_flamegraph(tmp_sql, q, mode, t, RESULT_DIR)

                # 4. Clean up temporary SQL file
                os.remove(tmp_sql)
                print(f"Cleaned up temporary SQL file: {tmp_sql}")
                print("=" * 50)


if __name__ == "__main__":
    main()