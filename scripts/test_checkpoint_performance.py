import subprocess
import re
import csv
import os

# 配置
TEST_FILE = "pixels-retina/src/test/java/io/pixelsdb/pixels/retina/TestRetinaCheckpoint.java"
RATIOS = [0.01, 0.05, 0.10, 0.20, 0.50]
OUTPUT_CSV = "checkpoint_perf_results.csv"

# 正则表达式用于提取结果
PATTERNS = {
    "Target Delete Ratio": r"Target Delete Ratio: ([\d.]+)%",
    "Actual Ratio": r"Actual Ratio: ([\d.]+)%",
    "Offload Time (ms)": r"Total Offload Time:\s+([\d.]+) ms",
    "File Size (MB)": r"Checkpoint File Size:\s+([\d.]+) MB",
    "Offload Peak Mem (MB)": r"Offload Peak Mem Overhead:\s+([\d.]+) MB",
    "Write Throughput (MB/s)": r"Write Throughput:\s+([\d.]+) MB/s",
    "Cold Load Time (ms)": r"First Load Time \(Cold\):\s+([\d.]+) ms",
    "Load Memory (MB)": r"Load Memory Overhead:\s+([\d.]+) MB",
    "Read Throughput (MB/s)": r"Read/Parse Throughput:\s+([\d.]+) MB/s",
    "Avg Memory Hit Latency (ms)": r"Avg Memory Hit Latency:\s+([\d.]+) ms"
}

def run_maven_test(ratio):
    print(f"\n>>> Running test for Target Delete Ratio: {ratio*100:.2f}%")
    
    # 1. 修改源码中的 ratio
    with open(TEST_FILE, 'r') as f:
        content = f.read()
    
    new_content = re.sub(
        r"double targetDeleteRatio = [\d.]+; // @TARGET_DELETE_RATIO@",
        f"double targetDeleteRatio = {ratio}; // @TARGET_DELETE_RATIO@",
        content
    )
    
    with open(TEST_FILE, 'w') as f:
        f.write(new_content)
    
    # 2. 执行命令
    cmd = [
        "mvn", "test",
        "-Dtest=TestRetinaCheckpoint#testCheckpointPerformance",
        "-pl", "pixels-retina",
        "-DargLine=-Xms40g -Xmx40g"
    ]
    env = os.environ.copy()
    env["LD_PRELOAD"] = "/lib/x86_64-linux-gnu/libjemalloc.so.2"
    
    try:
        process = subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        full_output = []
        for line in process.stdout:
            print(line, end="")
            full_output.append(line)
        process.wait()
        
        if process.returncode != 0:
            print(f"Error: Test failed for ratio {ratio}")
            return None
            
        # 3. 解析输出
        output_str = "".join(full_output)
        results = {"Target Delete Ratio": ratio * 100}
        for key, pattern in PATTERNS.items():
            if key == "Target Delete Ratio": continue
            match = re.search(pattern, output_str)
            if match:
                results[key] = match.group(1)
            else:
                results[key] = "N/A"
        
        return results

    except Exception as e:
        print(f"Exception during test: {e}")
        return None

def main():
    all_results = []
    
    # 备份原始文件
    with open(TEST_FILE, 'r') as f:
        original_content = f.read()
    
    try:
        for ratio in RATIOS:
            res = run_maven_test(ratio)
            if res:
                all_results.append(res)
                
        # 写入 CSV
        if all_results:
            keys = all_results[0].keys()
            with open(OUTPUT_CSV, 'w', newline='') as f:
                dict_writer = csv.DictWriter(f, fieldnames=keys)
                dict_writer.writeheader()
                dict_writer.writerows(all_results)
            print(f"\nSuccess! Results saved to {OUTPUT_CSV}")
        else:
            print("\nNo results collected.")
            
    finally:
        # 还原原始文件
        with open(TEST_FILE, 'w') as f:
            f.write(original_content)

if __name__ == "__main__":
    main()
