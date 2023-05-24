#include <iostream>
#include <string>

#include "cli/cli.h"
#include "cli/clifilesession.h"
#include "yaml-cpp/yaml.h"
#include "spdlog/spdlog.h"

#include "grpc/transpile_sql_client.h"
#include "db/duckdb_manager.h"

using namespace cli;

int main() {
    YAML::Node config = YAML::LoadFile("config.yaml");

    // Init an in-memory DuckDB instance
    // TODO: specify disk store
    DuckDBManager db_manager(":memory:");
    db_manager.importSqlFile("./resources/tpch_schema.sql");
    db_manager.importSqlFile("./resources/tpch_sample.sql");

//    auto result = db_manager.executeQuery("SELECT * FROM duckdb_tables();");
//    db_manager.executeQuery("SELECT * FROM table;");
//    result->Print();

    std::string host_addr = config["server_address"].as<std::string>();
    std::string port = config["server_port"].as<std::string>();
    TranspileSqlClient client(grpc::CreateChannel(host_addr + ":" + port, grpc::InsecureChannelCredentials()));
    spdlog::info("Connected to server: {}", host_addr + ":" + port);

    std::string token = "";
    std::string from_dialect = "duckdb";
    std::string to_dialect = "hive";

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
                    std::string transpiled_result = client.TranspileSql(token, query, from_dialect, to_dialect);
                    out << "Transpiled SQL query is: " << transpiled_result << std::endl;
                } catch (const std::exception& e) {
                    std::cerr << "Caught exception: " << e.what() << std::endl;
                    spdlog::error("Caught transpile exception: {}", e.what());
                }
            },
            "Call TranspileSQL Service");

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

    rootMenu->Insert(std::move(duckdbMenu));

    Cli cli(std::move(rootMenu));

    // global exit action
    cli.ExitAction([](auto& out){
        out << "Exit worker-cli and disconnected with Pixels." << std::endl;
    });

    CliFileSession input(cli);
    input.Start();

    return 0;
}
