# Copyright 2023 PixelsDB.

# This file is part of Pixels.

# Pixels is free software: you can redistribute it and/or modify
# it under the terms of the Affero GNU General Public License as
# published by the Free Software Foundation, either version 3 of
# the License, or (at your option) any later version.

# Pixels is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# Affero GNU General Public License for more details.

# You should have received a copy of the Affero GNU General Public
# License along with Pixels.  If not, see
# <https://www.gnu.org/licenses/>.

import json
import argparse
from sqlglot import parse_one, exp
from pulp import *
from typing import Dict, List
import yaml


def read_yaml_config(path):
    with open(path, "r") as file:
        try:
            return yaml.safe_load(file)
        except yaml.YAMLError as error:
            print(error)


# Given a query, parse and return the columns in the query
def get_columns(schema: Dict[str, List[str]], query: str) -> List[str]:
    all_table_columns = [col for sublist in schema.values() for col in sublist]
    column_list = []
    for column in parse_one(query).find_all(exp.Column):
        if column.alias_or_name in all_table_columns:
            column_list.append(column.alias_or_name)
    return list(set(column_list))


# Match a list of columns to the schema and return the corresponding partial schema
def collist_to_partial_schema(
        schema: Dict[str, List[str]], collist: List[str]
) -> Dict[str, List[str]]:
    partial_schema = {}
    for table, columns in schema.items():
        partial_schema[table] = []
        for column in columns:
            if column in collist:
                partial_schema[table].append(column)
    return partial_schema


# Entry point to call different strategies
def plan_cache_columns(
        strategy: str,
        schema: Dict[str, List[str]],
        stat: Dict[str, int],
        workload: List[str],
        workload_cost: List[int],
        storage_restriction: int,
) -> Dict[str, List[str]]:
    # two baseline methods: most number / most frequent
    if strategy == "most_number_columns":
        return cache_most_columns(schema, stat, workload, storage_restriction)
    elif strategy == "most_frequent_columns":
        return cache_most_frequent_columns(schema, stat, workload, storage_restriction)
    elif strategy == "rate_greedy_columns":
        return cache_rate_greedy_columns(schema, stat, workload, storage_restriction)
    elif strategy == "most_coverage_columns":
        return cache_most_coverage_columns(schema, stat, workload, storage_restriction)
    elif strategy == "cost_optimal_columns":
        return cache_cost_optimal_columns(
            schema, stat, workload, workload_cost, storage_restriction
        )


# Cache as much columns as possible based on the column statistics
def cache_most_columns(
        schema: Dict[str, List[str]],
        stat: Dict[str, int],
        workload: List[str],
        storage_restriction: int,
) -> Dict[str, List[str]]:
    # Sort the columns based on the column size
    sorted_stat = sorted(stat.items(), key=lambda x: x[1])

    # Cache the columns until the storage restriction is reached
    cache_columns = []
    for column, size in sorted_stat:
        if size <= storage_restriction:
            cache_columns.append(column)
            storage_restriction -= size
        else:
            break

    return collist_to_partial_schema(schema, cache_columns)


# Cache the columns that are most frequently used in the workload
def cache_most_frequent_columns(
        schema: Dict[str, List[str]],
        stat: Dict[str, int],
        workload: List[str],
        storage_restriction: int,
) -> Dict[str, List[str]]:
    # Get the columns frequency in the workload
    col_freq = {}
    for query in workload:
        for col in get_columns(schema, query):
            if col in col_freq:
                col_freq[col] += 1
            else:
                col_freq[col] = 1

    # Sort the columns based on the frequency
    sorted_freq = sorted(col_freq.items(), key=lambda x: x[1], reverse=True)
    print("The frequency of columns in the workload: ", sorted_freq)

    # Cache the most frequent columns until the storage restriction is reached
    cache_columns = []
    for column, freq in sorted_freq:
        if stat[column] <= storage_restriction:
            cache_columns.append(column)
            storage_restriction -= stat[column]
        else:
            continue

    return collist_to_partial_schema(schema, cache_columns)


# Cache the columns greedy based on the rate of size to frequency
def cache_rate_greedy_columns(
        schema: Dict[str, List[str]],
        stat: Dict[str, int],
        workload: List[str],
        storage_restriction: int,
) -> Dict[str, List[str]]:
    # Get the columns frequency in the workload
    col_freq = {}
    for query in workload:
        for col in get_columns(schema, query):
            if col in col_freq:
                col_freq[col] += 1
            else:
                col_freq[col] = 1

    # Sort the columns based on the frequency / size
    sorted_rate = sorted(
        [(col, col_freq[col] / stat[col]) for col in col_freq],
        key=lambda x: x[1],
        reverse=True,
    )
    print("The rate of columns in the workload: ", sorted_rate)

    # Cache the most frequent columns until the storage restriction is reached
    cache_columns = []
    for column, rate in sorted_rate:
        if stat[column] <= storage_restriction:
            cache_columns.append(column)
            storage_restriction -= stat[column]
        else:
            continue

    return collist_to_partial_schema(schema, cache_columns)


# Cache to make the most coverage of the workload (assumed all queries are equally costly)
# The problem can be formed as a linear programming problem
def cache_most_coverage_columns(
        schema: Dict[str, List[str]],
        stat: Dict[str, int],
        workload: List[str],
        storage_restriction: int,
) -> Dict[str, List[str]]:
    col_name = list(stat.keys())
    col_index_dict = dict(zip(col_name, range(1, len(col_name) + 1)))
    index_col_dict = dict(zip(range(1, len(col_name) + 1), col_name))

    # Mapping from workload index to column names (increasing order)
    mapping = {}
    for i in range(1, len(workload) + 1):
        mapping[i] = sorted(
            [col_index_dict[name] for name in get_columns(schema, workload[i - 1])]
        )

    aij_list = []
    for i in range(1, len(workload) + 1):
        for j in mapping[i]:
            aij_list.append("q" + str(i) + "_" + str(index_col_dict[j]))

    # Define the MILP problem
    prob = LpProblem("Cache Coverage", LpMaximize)

    a = LpVariable.dicts("a", aij_list, cat="Binary")
    x = LpVariable.dicts("x", col_name, cat="Binary")
    y = LpVariable.dicts("y", range(1, len(workload) + 1), cat="Binary")

    # Objective function: maximize the number of queries covered
    prob += lpSum([y[i] for i in range(1, len(workload) + 1)])

    # Constraints
    for yi, col_ids in mapping.items():
        for col in col_ids:
            prob += y[yi] <= a["q" + str(yi) + "_" + str(index_col_dict[col])]

    for col in range(1, len(col_name) + 1):
        for yi, col_ids in mapping.items():
            if col in col_ids:
                prob += (
                        x[index_col_dict[col]]
                        >= a["q" + str(yi) + "_" + str(index_col_dict[col])]
                )

    # Storage restriction
    prob += (
            lpSum(
                [
                    stat[index_col_dict[i]] * x[index_col_dict[i]]
                    for i in range(1, len(col_name) + 1)
                ]
            )
            <= storage_restriction
    )
    prob.solve()

    # Return the columns that are cached
    cache_columns = []
    for i in range(1, len(col_name) + 1):
        if x[index_col_dict[i]].varValue == 1:
            cache_columns.append(index_col_dict[i])

    return collist_to_partial_schema(schema, cache_columns)


# Cache to make the optimal cost of the workload
def cache_cost_optimal_columns(
        schema: Dict[str, List[str]],
        stat: Dict[str, int],
        workload: List[str],
        workload_cost: List[int],
        storage_restriction: int,
) -> Dict[str, List[str]]:
    col_name = list(stat.keys())
    col_index_dict = dict(zip(col_name, range(1, len(col_name) + 1)))
    index_col_dict = dict(zip(range(1, len(col_name) + 1), col_name))

    # Mapping from workload index to column names (increasing order)
    mapping = {}
    for i in range(1, len(workload) + 1):
        mapping[i] = sorted(
            [col_index_dict[name] for name in get_columns(schema, workload[i - 1])]
        )

    aij_list = []
    for i in range(1, len(workload) + 1):
        for j in mapping[i]:
            aij_list.append("q" + str(i) + "_" + str(index_col_dict[j]))

    # Define the MILP problem
    prob = LpProblem("Cache Coverage", LpMinimize)

    a = LpVariable.dicts("a", aij_list, cat="Binary")
    x = LpVariable.dicts("x", col_name, cat="Binary")
    y = LpVariable.dicts("y", range(1, len(workload) + 1), cat="Binary")

    # Objective function: minimize the in-cloud computation cost
    prob += lpSum(
        [workload_cost[i - 1] * (1 - y[i]) for i in range(1, len(workload) + 1)]
    )

    # Constraints
    for yi, col_ids in mapping.items():
        for col in col_ids:
            prob += y[yi] <= a["q" + str(yi) + "_" + str(index_col_dict[col])]

    for col in range(1, len(col_name) + 1):
        for yi, col_ids in mapping.items():
            if col in col_ids:
                prob += (
                        x[index_col_dict[col]]
                        >= a["q" + str(yi) + "_" + str(index_col_dict[col])]
                )

    # Storage restriction
    prob += (
            lpSum(
                [
                    stat[index_col_dict[i]] * x[index_col_dict[i]]
                    for i in range(1, len(col_name) + 1)
                ]
            )
            <= storage_restriction
    )
    prob.solve()

    # Return the columns that are cached
    cache_columns = []
    for i in range(1, len(col_name) + 1):
        if x[index_col_dict[i]].varValue == 1:
            cache_columns.append(index_col_dict[i])

    return collist_to_partial_schema(schema, cache_columns)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Read YAML config file.")
    parser.add_argument("--config", type=str, help="Path to the config file.")
    args = parser.parse_args()

    # Load the configuration file
    config = read_yaml_config(args.config)
    benchmark_path = config["benchmark_path"]
    schema_path = benchmark_path + config["schema_path"]
    table_stat_path = benchmark_path + config["table_stat_path"]
    workload_path = benchmark_path + config["workload_path"]
    cache_plan_path = benchmark_path + config["cache_plan_path"]
    workload_cost_path = benchmark_path + config["workload_cost_path"]

    # Load the schema (table name: List[column name])
    with open(schema_path) as f:
        schema = json.load(f)

    # Load the table statistics (column name: column size)
    with open(table_stat_path) as f:
        table_stat = json.load(f)
    col_name = table_stat["COL_NAME"]
    col_size = table_stat["COL_SIZE"]
    col_stat = dict(zip(col_name, col_size))

    # Load the workload queries (List[query])
    with open(workload_path) as f:
        queries = [line.strip() for line in f]

    # Load the workload cost from json file
    with open(workload_cost_path) as f:
        workload_cost = list(json.load(f).values())[0]

    # Load the strategy and storage restriction
    strategy = config["strategy"]
    storage_restriction = (int)(config["storage_restriction"])

    # Print the total size
    print("The total size of the columns: ", sum(col_size))

    # Print the respective column size percentage of each workload query
    total_size = sum(col_size)
    for i, query in enumerate(queries):
        print(
            "The size percentage of columns in query",
            i,
            ":",
            sum([col_stat[col] for col in get_columns(schema, query)])
            / total_size
            * 100,
        )

    # Plan the cache columns and write to json file
    cache_plan = plan_cache_columns(
        strategy, schema, col_stat, queries, workload_cost, storage_restriction
    )
    print("The columns planned to cache: ", cache_plan)
    with open(cache_plan_path, "w") as f:
        json.dump(cache_plan, f, indent=4)
