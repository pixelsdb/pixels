#ifndef PIXELS_WORKER_AMPHI_DUCKDB_MANAGER_H
#define PIXELS_WORKER_AMPHI_DUCKDB_MANAGER_H

#include <string>
#include <memory>
#include <fstream>
#include <sstream>
#include <iostream>

#include "duckdb.hpp"
#include "spdlog/spdlog.h"

class DuckDBManager {
private:
    std::unique_ptr<duckdb::DuckDB> db;
    std::unique_ptr<duckdb::Connection> con;

public:
    DuckDBManager(const std::string &db_path);
    ~DuckDBManager();

    std::unique_ptr<duckdb::MaterializedQueryResult> executeQuery(const std::string &query);
    void importSqlFile(const std::string& file_path);
};

#endif //PIXELS_WORKER_AMPHI_DUCKDB_MANAGER_H
