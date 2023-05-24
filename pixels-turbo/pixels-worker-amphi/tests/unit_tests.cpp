#include <iostream>
#include <gtest/gtest.h>

#include "duckdb.hpp"
#include "db/duckdb_manager.h"
#include "grpc/transpile_sql_client.h"
#include "exception/grpc_transpile_exception.h"
#include "yaml-cpp/yaml.h"
#include "spdlog/spdlog.h"

YAML::Node config = YAML::LoadFile("../../config.yaml");

TEST(grpc, transpileTest) {
    std::string host_addr = config["server_address"].as<std::string>();
    std::string port = config["server_port"].as<std::string>();
    TranspileSqlClient client(grpc::CreateChannel(host_addr + ":" + port, grpc::InsecureChannelCredentials()));
    spdlog::info("Connected to server: {}", host_addr + ":" + port);

    std::string token = "";
    std::string sql_statement = "SELECT STRFTIME(x, '%y-%-m-%S');";
    std::string from_dialect = "duckdb";
    std::string to_dialect = "hive";
    std::string expected_sql = "SELECT DATE_FORMAT(x, 'yy-M-ss')";
    spdlog::info("Sending TranspileSql request: ({} -> {}) {}", from_dialect, to_dialect, sql_statement);

    std::string transpiled_sql = client.TranspileSql(token, sql_statement, from_dialect, to_dialect);
    EXPECT_EQ(transpiled_sql, expected_sql);
    spdlog::info("Get expected transpiled SQL: {}", transpiled_sql);
}

TEST(grpc, transpileExceptionTest) {
    std::string host_addr = config["server_address"].as<std::string>();
    std::string port = config["server_port"].as<std::string>();
    TranspileSqlClient client(grpc::CreateChannel(host_addr + ":" + port, grpc::InsecureChannelCredentials()));
    spdlog::info("Connected to server: {}", host_addr + ":" + port);

    std::string token = "";
    std::string sql_statement = "SELECT * FROM;";
    std::string from_dialect = "duckdb";
    std::string to_dialect = "hive";
    spdlog::info("Sending TranspileSql request: ({} -> {}) {}", from_dialect, to_dialect, sql_statement);

    EXPECT_THROW(client.TranspileSql(token, sql_statement, from_dialect, to_dialect), GrpcTranspileException);
    spdlog::info("Expected to throw GrpcTranspileException");
}

TEST(DuckDBManager, importTpchSchemaTest) {
    DuckDBManager db_manager(":memory:");
    db_manager.importSqlFile("../../resources/tpch_schema.sql");

    auto tables_result = db_manager.executeQuery("SELECT * FROM duckdb_tables();");
    tables_result->Print();
    EXPECT_EQ(tables_result->RowCount(), 8);
    spdlog::info("TPC-H metadata should have 8 tables.");

    auto customer_columns_result = db_manager.executeQuery("PRAGMA table_info('customer');");
    customer_columns_result->Print();
    EXPECT_EQ(customer_columns_result->RowCount(), 8);
    spdlog::info("Customer table should have 8 columns.");
}

TEST(DuckDBManager, executeQueryErrorTest) {
    DuckDBManager db_manager(":memory:");
    db_manager.importSqlFile("../../resources/tpch_schema.sql");

    auto wrong_query_result = db_manager.executeQuery("SELECT c_name, n_name FROM customer;");
    EXPECT_EQ(wrong_query_result->HasError(), true);
    spdlog::info("DuckDB should report problematic query execution in stderr and log.");
}

TEST(DuckDBManager, executeQueryTpchSampleTest) {
    DuckDBManager db_manager(":memory:");
    db_manager.importSqlFile("../../resources/tpch_schema.sql");

    const std::string nation_q1 = "INSERT INTO nation (n_nationkey, n_name, n_regionkey, n_comment) \n"
            "VALUES (1, 'ARGENTINA', 1, 'al foxes promise slyly according to the regular accounts. bold requests alon');";
    const std::string nation_q2 = "INSERT INTO nation (n_nationkey, n_name, n_regionkey, n_comment) \n"
            "VALUES (2, 'BRAZIL', 2, 'y alongside of the pending deposits. carefully special packages are about the ironic forges');";
    const std::string nation_q3 = "INSERT INTO nation (n_nationkey, n_name, n_regionkey, n_comment) \n"
            "VALUES (3, 'CANADA', 3, 'eas hang ironic, silent packages. slyly regular packages are furiously over the tithes.');";

    db_manager.executeQuery(nation_q1);
    db_manager.executeQuery(nation_q2);
    db_manager.executeQuery(nation_q3);

    auto nation_all_query = db_manager.executeQuery("SELECT * FROM nation;");
    nation_all_query->Print();
    EXPECT_EQ(nation_all_query->RowCount(), 3);
    spdlog::info("Nation table should have 3 tuples.");

    auto nation_conditional_query = db_manager.executeQuery("SELECT * FROM nation WHERE n_name LIKE 'A%';");
    nation_conditional_query->Print();
    EXPECT_EQ(nation_conditional_query->RowCount(), 1);
    EXPECT_EQ(nation_conditional_query->GetValue(1, 0), "ARGENTINA");
    spdlog::info("Conditional query should result in ARGENTINA in n_name.");
}