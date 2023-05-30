#include <iostream>
#include <string>

#include "cli/cli.h"
#include "cli/clifilesession.h"
#include "yaml-cpp/yaml.h"
#include "spdlog/spdlog.h"

#include "grpc/transpile_sql_client.h"
#include "grpc/trino_query_client.h"
#include "db/duckdb_manager.h"

using namespace cli;

int main() {
    YAML::Node config = YAML::LoadFile("config.yaml");

    // Init an in-memory DuckDB instance
    DuckDBManager db_manager(":memory:");
    db_manager.importSqlFile("./resources/tpch_schema.sql");

    // Connect to pixels-server
    std::string host_addr = config["server_address"].as<std::string>();
    std::string port = config["server_port"].as<std::string>();
    TranspileSqlClient transpileSqlClient(grpc::CreateChannel(host_addr + ":" + port, grpc::InsecureChannelCredentials()));
    spdlog::info("Connected to server: {}", host_addr + ":" + port);

    // Configure transpile request
    std::string token = "";
    std::string from_dialect = "duckdb";
    std::string to_dialect = "hive";
    spdlog::info("Configured the SQL dialect transpile as {} -> {}", from_dialect, to_dialect);

    // Configure trino endpoint
    std::string trino_url = config["trino_url"].as<std::string>();
    int trino_port = config["trino_port"].as<int>();
    std::string trino_default_catalog = config["trino_default_catalog"].as<std::string>();
    std::string trino_default_schema = config["trino_default_schema"].as<std::string>();
    TrinoQueryClient trinoQueryClient(grpc::CreateChannel(host_addr + ":" + port, grpc::InsecureChannelCredentials()));
    spdlog::info("Configured the trino query endpoint: {}:{}/{}/{}",
                 trino_url, trino_port, trino_default_catalog, trino_default_schema);

    // start the interactive cli menu
    auto rootMenu = std::make_unique<Menu>("worker-cli");
    rootMenu -> Insert(
            "hello",
            [](std::ostream& out){
                out << "Here is CLI root menu. It provides commands for system setting and direct RPC call." << std::endl;
                },
            "Print the worker-cli menu functionality");
    rootMenu->Insert(
            "transpile",
            [&](std::ostream& out, std::string query){
                try {
                    std::string transpiled_result = transpileSqlClient.TranspileSql(token, query, from_dialect, to_dialect);
                    out << "Transpiled SQL query is: " << transpiled_result << std::endl;
                } catch (const std::exception& e) {
                    std::cerr << "Caught exception: " << e.what() << std::endl;
                    spdlog::error("Caught transpile exception: {}", e.what());
                }
            },
            "Call TranspileSQL Service");
    rootMenu->Insert(
            "trino",
            [&](std::ostream& out, std::string query){
                try {
                    std::string query_result = trinoQueryClient.TrinoQuery(token, trino_url, trino_port,
                                                                 trino_default_catalog, trino_default_schema, query);
                    out << "Trino query result: \n" << query_result << std::endl;
                } catch (const std::exception& e) {
                    std::cerr << "Caught exception: " << e.what() << std::endl;
                    spdlog::error("Caught trino query exception: {}", e.what());
                }
            },
            "Send SQL query to trino endpoint");

    rootMenu->Insert(
            "color",
            [](std::ostream& out){
                out << "Colors ON\n"; SetColor();
                },
            "Enable colors in the cli");
    rootMenu->Insert(
            "nocolor",
            [](std::ostream& out){
                out << "Colors OFF\n"; SetNoColor();
                },
            "Disable colors in the cli");

    auto duckdbMenu = std::make_unique<Menu>("duckdb");
    duckdbMenu->Insert(
            "hello",
            [](std::ostream& out){
                out << "Here is DuckDB CLI. You can run SQL queries in the local DuckDB." << std::endl;
                },
            "Print the DuckDB menu functionality");
    duckdbMenu->Insert(
            "run",
            [&db_manager](std::ostream& out, std::string query){
                auto result = db_manager.executeQuery(query);
                result->Print();
            },
            "Execute single query in DuckDB instance");
    duckdbMenu->Insert(
            "import",
            [&db_manager](std::ostream& out, std::string file_path){
                db_manager.importSqlFile(file_path);
            },
            "Import SQL file in DuckDB instance");

    rootMenu->Insert(std::move(duckdbMenu));

    Cli cli(std::move(rootMenu));

    // global exit action
    cli.ExitAction([](auto& out){
        out << "bye~" << std::endl;
    });

    CliFileSession input(cli);
    input.Start();

    return 0;
}
