import subprocess
import time
import re
import csv
import argparse
from collections import Counter
import os # <-- import os model


def clear_page_cache():
    """Clear Linux page cache to ensure fair benchmarking"""
    try:
        print("🧹 Clearing Linux page cache...")
        # Synchronize filesystem caches
        subprocess.run(["sync"], check=True)
        # Drop caches (3 clears pagecache, dentries, and inodes)
        subprocess.run(["sudo", "bash", "-c", "echo 3 > /proc/sys/vm/drop_caches"], check=True)
        print("✅ Page cache cleared successfully")
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Failed to clear page cache: {e}")


# -------------------- 1️⃣ Parse Command Line Arguments --------------------
parser = argparse.ArgumentParser(description="Monitor I/O granularity using blktrace and blkparse")
parser.add_argument("--benchmark", required=True, help="Benchmark name, used as output file prefix")
args = parser.parse_args()
benchmark_name = args.benchmark

# -------------------- 2️⃣ Define Regex Pattern --------------------
# Pattern for capturing I/O size (in sectors) and the process name
# The current pattern targets 'G' (Get request) operations.
pattern = re.compile(r"\sG\s+RA?\s+\d+\s+\+\s+(\d+)\s+\[(duckdb|iou-sqp-\d+)\]")

# -------------------- 3️⃣ Start blktrace and blkparse Pipeline --------------------
# blktrace monitors block device I/O on nvme0n1 and outputs raw data to stdout
blktrace_cmd = ["sudo", "blktrace", "-d", "/dev/nvme0n1","-o", "-"]
# blkparse reads raw data from stdin ('-')
blkparse_cmd = ["blkparse", "-i", "-"]

p1 = subprocess.Popen(blktrace_cmd, stdout=subprocess.PIPE)
p2 = subprocess.Popen(blkparse_cmd, stdin=p1.stdout, stdout=subprocess.PIPE, text=True)

# -------------------- 4️⃣ Clear Page Cache --------------------
clear_page_cache()

# -------------------- 5️⃣ Start Benchmark Script (process_sqls.py) --------------------
proc = subprocess.Popen(["python3", "process_sqls.py", "--runs", "1", "--benchmark", benchmark_name])

# -------------------- 6️⃣ Real-time I/O Granularity Collection --------------------
counter = Counter()
print(f"📊 Collecting I/O traces while benchmark '{benchmark_name}' is running...")

try:
    # Read blkparse output line by line
    for line in p2.stdout:
        # Search for I/O size and process name using the defined pattern
        match = pattern.search(line)

        if match:
            # Group 1 is the I/O size in sectors
            size = int(match.group(1))
            counter[size] += 1

        # Check if the benchmark process (process_sqls) has finished
        if proc.poll() is not None:
            break
except KeyboardInterrupt:
    print("⏹️ Stopped manually")

# -------------------- 7️⃣ Terminate blktrace/blkparse --------------------
p1.terminate()
p2.terminate()

# -------------------- 8️⃣ Create Output Directory and Save Results --------------------
output_dir = "io_results"
output_filename = os.path.join(output_dir, f"io_granularity_stats-{benchmark_name}.csv")

# --- check and make directory ---
if not os.path.exists(output_dir):
    print(f"📁 Output directory '{output_dir}' not found. Creating it...")
    # recursively create directories if they don't exist
    os.makedirs(output_dir)
# ----------------------

with open(output_filename, "w", newline="") as f:
    writer = csv.writer(f)
    # Write header: IO size in sectors, count of requests, and IO size converted to bytes (512 bytes/sector)
    writer.writerow(["IO_Size_Sectors", "Count", "IO_Size_Bytes"])
    # Write sorted results
    for s, c in sorted(counter.items()):
        writer.writerow([s, c, s * 512])

print(f"✅ Results saved to {output_filename}")



