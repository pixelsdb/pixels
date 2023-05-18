#include <iostream>
#include <string>

#include "duckdb.hpp"
#include "yaml-cpp/yaml.h"

#include "worker/include/grpc/transpile_sql_client.h"

int main() {
    YAML::Node config = YAML::LoadFile("config.yaml");

    // DuckDB example
    duckdb::DuckDB db(nullptr);
    duckdb::Connection con(db);
    con.Query("CREATE TABLE integers(i INTEGER)");
    con.Query("INSERT INTO integers VALUES (3)");
    auto result = con.Query("SELECT * FROM integers");
    result->Print();

    // gRPC example
    std::string host_addr = config["server_address"].as<std::string>();
    std::string port = config["server_port"].as<std::string>();
    TranspileSqlClient client(grpc::CreateChannel(host_addr + ":" + port, grpc::InsecureChannelCredentials()));
    std::string token = "";
    std::string sql_statement = "SELECT STRFTIME(x, '%y-%-m-%S');";
    std::string from_dialect = config["local_engine"].as<std::string>();
    std::string to_dialect = config["remote_engine"].as<std::string>();

    std::string transpiled_sql = client.TranspileSql(token, sql_statement, from_dialect, to_dialect);

    std::cout << "Transpiled SQL: " << transpiled_sql << std::endl;

    return 0;
}
