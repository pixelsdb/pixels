import argparse
import json

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Read experiment results.')
    parser.add_argument('--result', type=str, help='Path to the result txt file.')
    parser.add_argument('--stat', type=str, help='Path to the cost stat json file.')
    args = parser.parse_args()

    with open(args.stat, 'r') as f:
        stat = list(json.load(f).values())[0]
    
    with open(args.result, 'r') as f:
        lines = f.readlines()
    
    execution_time = []
    execution_location = []
    for i, line in enumerate(lines):
        if i % 2 == 0:
            execution_location.append(line.strip())
        else:
            execution_time.append(int(line.strip()))
    
    worker_time = sum([execution_time[i] for i in range(len(execution_time)) if execution_location[i] == 'worker'])
    cloud_time = sum([execution_time[i] for i in range(len(execution_time)) if execution_location[i] == 'cloud'])

    print("Worker time: {} s".format(worker_time))
    print("Cloud time: {} s".format(cloud_time))

    total_cost = sum(stat)
    worker_cost = sum([stat[i] for i in range(len(stat)) if execution_location[i] == 'worker'])
    cloud_cost = sum([stat[i] for i in range(len(stat)) if execution_location[i] == 'cloud'])

    # Print the cost percentage
    print("Worker cost: {}%".format(worker_cost / total_cost * 100))
    print("Cloud cost: {}%".format(cloud_cost / total_cost * 100))
    cloud_queries = list(filter(lambda x: x == 'cloud', execution_location))
    print("In-cloud query percentage: {}%".format(len(cloud_queries) / len(execution_location) * 100))