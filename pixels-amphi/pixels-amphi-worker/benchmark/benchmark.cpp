/*
 * Copyright 2023 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
#include <iostream>
#include <string>

#include "yaml-cpp/yaml.h"
#include "spdlog/spdlog.h"
#include "aws/core/Aws.h"

#include "grpc/transpile_sql_client.h"
#include "grpc/trino_query_client.h"
#include "db/duckdb_manager.h"

int main() {
    std::cout << "Hello benchmark" << std::endl;

    // Load benchmark configuration (one argument to specify configuration path)



    // Register the peer and report peer paths (assumed that we have cached the data)



    // Create table on the cached parquet folders with DuckDB instance



    // Iterate the workload sql queries, send coordinator query request
    // If no error and in-cloud false, execute the query with DuckDB


    // Log both the result and time cost



}