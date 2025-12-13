import subprocess
import os
import re
import shutil

# -------------------------------------
# 1. Configuration Parameters
# -------------------------------------
# threads_list = [1, 2, 4, 8, 16, 24, 48, 64, 96]
threads_list = [16, 24]
benchmarks = [
    "clickbench-pixels-e0-1ssd",
    # "clickbench-pixels-e0-6ssd",
    # "clickbench-pixels-e0-12ssd",
    # "clickbench-pixels-e0-24ssd"
]

runs = 1
# Define all Buffer Modes to be tested
buffer_modes = ["doublebuffer", "singlebuffer"]
properties_path = os.path.expanduser("~/opt/pixels/etc/pixels-cpp.properties")
process_script = "process_sqls.py"

# Root directory for saving results
output_root = "single_double_buffer_results"

# -------------------------------------
# 2. Core Modification: Buffer Mode Switching Function
# -------------------------------------
def set_buffer_mode(mode):
    """Modify the 'pixels.doublebuffer' parameter in pixels-cpp.properties"""
    assert mode in ("doublebuffer", "singlebuffer")

    if not os.path.exists(properties_path):
        raise FileNotFoundError(f"Configuration file not found: {properties_path}")

    with open(properties_path, "r") as f:
        lines = f.readlines()

    new_lines = []
    changed = False

    # Determine the value to set
    new_value = "true" if mode == "doublebuffer" else "false"

    for line in lines:
        if line.strip().startswith("pixels.doublebuffer"):
            # Find and replace this line
            new_lines.append(f"pixels.doublebuffer={new_value}\n")
            changed = True
        else:
            new_lines.append(line)

    # If the line was not found in the file, append it at the end
    if not changed:
        new_lines.append(f"pixels.doublebuffer={new_value}\n")

    with open(properties_path, "w") as f:
        f.writelines(new_lines)

    print(f"🔄 Buffer mode switched to: {mode.upper()}")


# -------------------------------------
# 3. IO Mode Switching Function (Unchanged logic)
# -------------------------------------
def set_io_mode(mode):
    """Modify the 'localfs.enable.async.io' parameter in pixels-cpp.properties"""
    assert mode in ("async", "sync")

    if not os.path.exists(properties_path):
        raise FileNotFoundError(f"Configuration file not found: {properties_path}")

    with open(properties_path, "r") as f:
        lines = f.readlines()

    new_lines = []
    changed = False
    for line in lines:
        if line.startswith("localfs.enable.async.io"):
            new_value = "true" if mode == "async" else "false"
            new_lines.append(f"localfs.enable.async.io={new_value}\n")
            changed = True
        else:
            new_lines.append(line)

    if not changed:
        new_value = "true" if mode == "async" else "false"
        new_lines.append(f"localfs.enable.async.io={new_value}\n")

    with open(properties_path, "w") as f:
        f.writelines(new_lines)

    print(f"🔧 IO mode switched to: {mode.upper()}")

# -------------------------------------
# 4. Nested Loop for Test Execution
# -------------------------------------
for buffer_mode in buffer_modes:
    print(f"\n===========================================")
    print(f"🚀 Starting Test for Buffer Mode: {buffer_mode.upper()}")
    print(f"===========================================")
    set_buffer_mode(buffer_mode) # <-- Set the current Buffer Mode

    for io_mode in ["sync", "async"]:
        print(f"\n======= Switching to {io_mode.upper()} mode =======")
        set_io_mode(io_mode)

        for benchmark in benchmarks:
            print(f"\n===== Benchmark: {benchmark} ({buffer_mode}/{io_mode}) =====\n")

            # Create an isolated directory: output_root/benchmark/buffer_mode/io_mode/
            benchmark_dir = os.path.join(output_root, benchmark, buffer_mode, io_mode)
            os.makedirs(benchmark_dir, exist_ok=True)
            print(f"📁 Directory created: {benchmark_dir}")

            for t in threads_list:
                output_csv = os.path.join(
                    benchmark_dir,
                    f"duckdb_benchmark_result-{buffer_mode}-{io_mode}-{t}threads.csv"
                )

                cmd = [
                    "python",
                    process_script,
                    "--benchmark", benchmark,
                    "--runs", str(runs),
                    "--output-csv", output_csv,
                    "--threads", str(t),
                ]

                print(f"\n▶ Executing: {benchmark}, Buffer={buffer_mode}, IO={io_mode}, {t} threads")
                print("Command:", " ".join(cmd))

                subprocess.run(cmd, check=True)

                print(f"✔ Completed: {output_csv}\n")

print("\n🎉 All tasks (doublebuffer/singlebuffer, sync/async) completed successfully!")
