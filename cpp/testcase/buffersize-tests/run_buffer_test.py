import os
import re
import subprocess
import sys
import csv

# Configuration
PROPERTIES_FILE = "/home/whz/opt/pixels/etc/pixels-cpp.properties"
RUN_PERF_SCRIPT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../run_perf.py"))
SQL_DIR = os.path.dirname(os.path.abspath(__file__))
RESULT_BASE_DIR = os.path.join(SQL_DIR, "results_buffer_test")
THREADS = [24]
QUERY = "q06"
SSD_MODE = "24ssd"

# Buffer sizes: 0 to 100MB (100 * 1024 * 1024)
MAX_SIZE = 300 * 1024 * 1024
STEP_SIZE = 50 * 1024 * 1024
SIZES = list(range(0, MAX_SIZE + 1, STEP_SIZE))

def update_property(file_path, key, value):
    """
    Updates the value of a key in the properties file.
    If the key doesn't exist, it appends it.
    If multiple entries exist, it updates the active one (uncommented).
    """
    try:
        with open(file_path, 'r') as f:
            lines = f.readlines()
    except FileNotFoundError:
        print(f"Error: Properties file not found at {file_path}")
        sys.exit(1)

    new_lines = []
    key_found = False
    
    # Regex to match "key=value" with optional whitespace
    # We want to match lines that are NOT commented out
    pattern = re.compile(rf"^\s*{re.escape(key)}\s*=")

    for line in lines:
        if pattern.match(line):
            new_lines.append(f"{key}={value}\n")
            key_found = True
        else:
            new_lines.append(line)

    if not key_found:
        new_lines.append(f"\n{key}={value}\n")

    with open(file_path, 'w') as f:
        # print(new_lines)
        f.writelines(new_lines)
    print(f"Updated {key} to {value} in {file_path}")

def parse_perf_stats(result_file):
    """
    Parses execution time and page-faults from the perf stat output file.
    Returns a dictionary with 'time', 'page_faults', 'minor_faults', 'major_faults'
    """
    stats = {
        'time': None,
        'page_faults': None,
        'minor_faults': None,
        'major_faults': None
    }
    
    try:
        with open(result_file, 'r') as f:
            for line in f:
                # Parse execution time
                if "seconds time elapsed" in line:
                    parts = line.split()
                    try:
                        stats['time'] = float(parts[0].replace(',', ''))
                    except (ValueError, IndexError):
                        pass
                
                # Parse page-faults (total)
                elif "page-faults" in line:
                    parts = line.split()
                    try:
                        stats['page_faults'] = int(parts[0].replace(',', ''))
                    except (ValueError, IndexError):
                        pass
                
                # Parse minor-faults
                elif "minor-faults" in line:
                    parts = line.split()
                    try:
                        stats['minor_faults'] = int(parts[0].replace(',', ''))
                    except (ValueError, IndexError):
                        pass
                
                # Parse major-faults
                elif "major-faults" in line:
                    parts = line.split()
                    try:
                        stats['major_faults'] = int(parts[0].replace(',', ''))
                    except (ValueError, IndexError):
                        pass
                        
    except FileNotFoundError:
        print(f"Warning: Result file {result_file} not found.")
    
    return stats

def main():
    # Switch to testcase directory so that run_perf.py can find relative resources
    # (duckdb binary and flamegraph script)
    testcase_dir = os.path.dirname(SQL_DIR)
    os.chdir(testcase_dir)
    print(f"Changed working directory to: {testcase_dir}")

    if not os.path.exists(RESULT_BASE_DIR):
        os.makedirs(RESULT_BASE_DIR)

    results = []

    print(f"Starting Buffer Size Test")
    print(f"Sizes to test: {[s // (1024*1024) for s in SIZES]} MB")
    print(f"SQL Dir: {SQL_DIR}")
    print("=" * 60)

    for size in SIZES:
        print(f"\n=== Testing Buffer Size: {size / (1024*1024):.1f} MB ===")
        
        # 1. Update properties
        update_property(PROPERTIES_FILE, "pixel.bufferpool.bufferpoolSize", size)

        # 2. Run run_perf.py
        current_result_dir = os.path.join(RESULT_BASE_DIR, f"size_{size}")
        
        # Construct command
        # python3 run_perf.py --sql-dir ... --result-dir ... --queries ... --ssd-modes ... --threads ...
        cmd = [
            "python3", RUN_PERF_SCRIPT,
            "--sql-dir", SQL_DIR,
            "--result-dir", current_result_dir,
            "--queries", QUERY,
            "--ssd-modes", SSD_MODE,
            "--threads", str(THREADS[0]) # Start with single thread arg if list
        ]
        
        # run_perf.py expects multiple threads as args if we passed list, but here we pass one
        
        try:
            subprocess.run(cmd, check=True)
            
            # 3. Collect statistics
            # The output file name format from run_perf.py:
            # f"{query_name}-{ssd_mode}-threads{thread_value}-context-stat.txt"
            stat_file = os.path.join(
                current_result_dir, 
                f"{QUERY}-{SSD_MODE}-threads{THREADS[0]}-context-stat.txt"
            )
            
            stats = parse_perf_stats(stat_file)
            if stats['time'] is not None:
                print(f"Execution Time: {stats['time']:.4f} seconds")
                print(f"Page Faults: {stats['page_faults']}")
                print(f"Minor Faults: {stats['minor_faults']}")
                print(f"Major Faults: {stats['major_faults']}")
                results.append({
                    'buffer_size': size,
                    'buffer_size_mb': size / (1024 * 1024),
                    'time': stats['time'],
                    'page_faults': stats['page_faults'],
                    'minor_faults': stats['minor_faults'],
                    'major_faults': stats['major_faults']
                })
            else:
                print("Failed to parse statistics.")
                results.append({
                    'buffer_size': size,
                    'buffer_size_mb': size / (1024 * 1024),
                    'time': None,
                    'page_faults': None,
                    'minor_faults': None,
                    'major_faults': None
                })
                
        except subprocess.CalledProcessError as e:
            print(f"Error executing run_perf.py for size {size}: {e}")
            results.append({
                'buffer_size': size,
                'buffer_size_mb': size / (1024 * 1024),
                'time': 'Error',
                'page_faults': 'Error',
                'minor_faults': 'Error',
                'major_faults': 'Error'
            })

    # Save results to CSV
    csv_file = os.path.join(RESULT_BASE_DIR, "buffer_size_test_results.csv")
    with open(csv_file, 'w', newline='') as f:
        fieldnames = ['buffer_size_bytes', 'buffer_size_mb', 'time_seconds', 
                     'page_faults', 'minor_faults', 'major_faults']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        
        writer.writeheader()
        for result in results:
            writer.writerow({
                'buffer_size_bytes': result['buffer_size'],
                'buffer_size_mb': result['buffer_size_mb'],
                'time_seconds': result['time'],
                'page_faults': result['page_faults'],
                'minor_faults': result['minor_faults'],
                'major_faults': result['major_faults']
            })
    
    print(f"\n==> Results saved to CSV: {csv_file}")

    # Print Summary
    print("\n" + "=" * 100)
    print("Buffer Size Test Summary")
    print("=" * 100)
    print(f"{'Buffer Size (MB)':<18} | {'Time (s)':<12} | {'Page Faults':<15} | {'Minor Faults':<15} | {'Major Faults':<15}")
    print("-" * 100)
    for result in results:
        size_mb = result['buffer_size_mb']
        time_val = result['time']
        page_faults = result['page_faults']
        minor_faults = result['minor_faults']
        major_faults = result['major_faults']
        
        if isinstance(time_val, float):
            print(f"{size_mb:<18.1f} | {time_val:<12.4f} | {page_faults:<15} | {minor_faults:<15} | {major_faults:<15}")
        else:
            print(f"{size_mb:<18.1f} | {str(time_val):<12} | {str(page_faults):<15} | {str(minor_faults):<15} | {str(major_faults):<15}")
    print("=" * 100)

if __name__ == "__main__":
    main()
