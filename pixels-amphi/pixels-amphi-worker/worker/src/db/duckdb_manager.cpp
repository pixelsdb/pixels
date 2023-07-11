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
#include "db/duckdb_manager.h"

DuckDBManager::DuckDBManager(const std::string &db_path) {
  db = std::make_unique<duckdb::DuckDB>(db_path);
  con = std::make_unique<duckdb::Connection>(*db);
  spdlog::info("DuckDB Manager init with path: {}", db_path);
}

DuckDBManager::~DuckDBManager() {}

// Execute single query
std::unique_ptr<duckdb::MaterializedQueryResult> DuckDBManager::executeQuery(
    const std::string &query) {
  std::unique_ptr<duckdb::MaterializedQueryResult> result = con->Query(query);

  if (result->HasError()) {
    std::cerr << "DuckDB executeQuery throws error: " << result->GetError()
              << std::endl;
    spdlog::error("DuckDB executeQuery throws error: {}", result->GetError());
  } else {
    spdlog::info("DuckDB successfully executed query: {}", query);
  }

  return result;
}

// Separate the file into multiple SQL queries and execute sequentially
void DuckDBManager::importSqlFile(const std::string &file_path) {
  std::ifstream file(file_path);

  if (!file.is_open()) {
    std::cerr << "DuckDB imported file not exists: " << file_path << std::endl;
    spdlog::error("DuckDB imported file not exists: {}", file_path);
  }

  std::stringstream buffer;
  buffer << file.rdbuf();
  file.close();

  std::string sql = buffer.str();
  std::string delimiter = ";";
  size_t pos = 0;
  std::string token;

  while ((pos = sql.find(delimiter)) != std::string::npos) {
    token = sql.substr(0, pos);
    executeQuery(token);
    sql.erase(0, pos + delimiter.length());
  }

  spdlog::info("DuckDB successfully imported SQL file: {}", file_path);
}
