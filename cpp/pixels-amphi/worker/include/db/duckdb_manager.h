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
#ifndef PIXELS_WORKER_AMPHI_DUCKDB_MANAGER_H
#define PIXELS_WORKER_AMPHI_DUCKDB_MANAGER_H

#include <string>
#include <memory>
#include <fstream>
#include <sstream>
#include <iostream>

#include "duckdb.hpp"
#include "spdlog/spdlog.h"

class DuckDBManager
{
private:
    std::unique_ptr <duckdb::DuckDB> db;
    std::unique_ptr <duckdb::Connection> con;

public:
    DuckDBManager(const std::string &db_path);

    ~DuckDBManager();

    std::unique_ptr <duckdb::MaterializedQueryResult> executeQuery(const std::string &query);

    void importSqlFile(const std::string &file_path);
};

#endif //PIXELS_WORKER_AMPHI_DUCKDB_MANAGER_H
