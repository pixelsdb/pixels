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
#include <filesystem>
#include <gtest/gtest.h>

#include "duckdb.hpp"
#include "db/duckdb_manager.h"
#include "grpc/transpile_sql_client.h"
#include "grpc/trino_query_client.h"
#include "exception/grpc_transpile_exception.h"
#include "utils/aws_utils.h"

#include "yaml-cpp/yaml.h"
#include "aws/core/Aws.h"
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

TEST(grpc, trinoQueryTest) {
    std::string host_addr = config["server_address"].as<std::string>();
    std::string port = config["server_port"].as<std::string>();

    std::string trino_url = config["trino_url"].as<std::string>();
    int trino_port = config["trino_port"].as<int>();
    std::string trino_default_catalog = config["trino_default_catalog"].as<std::string>();
    std::string trino_default_schema = config["trino_default_schema"].as<std::string>();
    TrinoQueryClient trinoQueryClient(grpc::CreateChannel(host_addr + ":" + port, grpc::InsecureChannelCredentials()));
    spdlog::info("Configured the trino query endpoint: {}:{}/{}/{}",
        trino_url, trino_port, trino_default_catalog, trino_default_schema);

    std::string token = "";
    std::string aggregate_query = "SELECT COUNT(*) AS item_count FROM LINEITEM";
    spdlog::info("Sending Trino query: {}", aggregate_query);

    std::string aggregate_query_result = trinoQueryClient.TrinoQuery(token, trino_url, trino_port,
                                                           trino_default_catalog, trino_default_schema, aggregate_query);
    if (trino_default_schema == "tpch_1g") {
        EXPECT_EQ(aggregate_query_result, "item_count: 6001215\n");
        spdlog::info("Get expected query result: {}", aggregate_query_result);
    }

    std::string tpch_query_22 = "select cntrycode, count(*) as numcust, sum(c_acctbal) as totacctbal from ( select substring( c_phone from 1 for 2 ) as cntrycode, c_acctbal from CUSTOMER where substring( c_phone from 1 for 2 ) in ('20', '40', '22', '30', '39', '42', '21') and c_acctbal > ( select avg(c_acctbal) from CUSTOMER where c_acctbal > 0.00 and substring( c_phone from 1 for 2 ) in ('20', '40', '22', '30', '39', '42', '21') ) and not exists ( select * from ORDERS where o_custkey = c_custkey ) ) as custsale group by cntrycode order by cntrycode";
    std::string tpch_query_result = trinoQueryClient.TrinoQuery(token, trino_url, trino_port,
                                                                 trino_default_catalog, trino_default_schema, tpch_query_22);
    if (trino_default_schema == "tpch_1g") {
        EXPECT_EQ(tpch_query_result, "cntrycode: 20, numcust: 916, totacctbal: 6824676.02\n"
                                     "cntrycode: 21, numcust: 955, totacctbal: 7235832.66\n"
                                     "cntrycode: 22, numcust: 893, totacctbal: 6631741.43\n"
                                     "cntrycode: 30, numcust: 910, totacctbal: 6813438.36\n");
        spdlog::info("Get expected query result: {}", tpch_query_result);
    }
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

TEST(DuckDBManager, readTpchParquetTest) {
    DuckDBManager db_manager(":memory:");

    Aws::SDKOptions options;
    Aws::InitAPI(options);
    Aws::Client::ClientConfiguration clientConfig;

    awsutils::GetObject("region/000000_0", "parquet-tpch", "./data", clientConfig);
    spdlog::info("Get object of region parquet file from s3.");

    // duckdb requires explicit parquet file name
    std::filesystem::path object_file_path = "./data/parquet-tpch/region/000000_0";
    std::filesystem::path parquet_file_path = "./data/parquet-tpch/region/000000_0.parquet";

    if(!std::filesystem::exists(object_file_path)) {
        spdlog::error("Failed to retrieve parquet file from s3: {}", object_file_path.string());
    }
    std::filesystem::rename(object_file_path, parquet_file_path);

    auto select_from_region_query = db_manager.executeQuery("SELECT * FROM '" + parquet_file_path.string() + "'");
    select_from_region_query->Print();
    spdlog::info("Query from parquet file in local file system.");
}

TEST(aws, s3UtilTest) {
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    Aws::Client::ClientConfiguration clientConfig;

    awsutils::ListBuckets(clientConfig);
    awsutils::ListObjects("pixels-tpch1g", clientConfig);

    awsutils::GetObject("nation/v-0-order/20230529201449_12.pxl", "pixels-tpch1g", "./data", clientConfig);
    spdlog::info("Get object of nation pxl file from s3.");

    Aws::ShutdownAPI(options);
}