#include <iostream>
#include <string>

#include "duckdb.hpp"
#include "yaml-cpp/yaml.h"
#include "spdlog/spdlog.h"

#include "grpc/transpile_sql_client.h"

int main() {
    YAML::Node config = YAML::LoadFile("config.yaml");

    // DuckDB example
    duckdb::DuckDB db(nullptr);
    duckdb::Connection con(db);
    con.Query("CREATE TABLE integers(i INTEGER)");
    con.Query("INSERT INTO integers VALUES (3)");
    auto result = con.Query("SELECT * FROM integers");
    result->Print();

    return 0;
}
