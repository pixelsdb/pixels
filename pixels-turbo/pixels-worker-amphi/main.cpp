#include <iostream>
#include "duckdb.hpp"
#include "worker/include/grpc/transpile_sql_client.h"

int main() {
    // DuckDB example
    duckdb::DuckDB db(nullptr);
    duckdb::Connection con(db);
    con.Query("CREATE TABLE integers(i INTEGER)");
    con.Query("INSERT INTO integers VALUES (3)");
    auto result = con.Query("SELECT * FROM integers");
    result->Print();

    // gRPC example
    std::string host_addr = "ec2-18-218-128-203.us-east-2.compute.amazonaws.com:18892";
    TranspileSqlClient client(grpc::CreateChannel(host_addr, grpc::InsecureChannelCredentials()));
    std::string token = "";
    std::string sql_statement = "SELECT STRFTIME(x, '%y-%-m-%S');";
    std::string from_dialect = "duckdb";
    std::string to_dialect = "hive";

    std::string transpiled_sql = client.TranspileSql(token, sql_statement, from_dialect, to_dialect);

    std::cout << "Transpiled SQL: " << transpiled_sql << std::endl;

    return 0;
}
