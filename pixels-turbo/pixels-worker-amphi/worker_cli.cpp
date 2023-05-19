#include <iostream>
#include <string>

#include "duckdb.hpp"
#include "cli/cli.h"
#include "cli/clifilesession.h"
#include "yaml-cpp/yaml.h"
#include "spdlog/spdlog.h"

#include "grpc/transpile_sql_client.h"

using namespace cli;
using namespace std;

int main() {
    YAML::Node config = YAML::LoadFile("config.yaml");

//    // DuckDB example
//    duckdb::DuckDB db(nullptr);
//    duckdb::Connection con(db);
//    con.Query("CREATE TABLE integers(i INTEGER)");
//    con.Query("INSERT INTO integers VALUES (3)");
//    auto result = con.Query("SELECT * FROM integers");
//    result->Print();

// init process
    std::string host_addr = config["server_address"].as<std::string>();
    std::string port = config["server_port"].as<std::string>();
    TranspileSqlClient client(grpc::CreateChannel(host_addr + ":" + port, grpc::InsecureChannelCredentials()));
    spdlog::info("Connected to server: {}", host_addr + ":" + port);

    std::string token = "";
    std::string sql_statement = "SELECT STRFTIME(x, '%y-%-m-%S');";
    std::string from_dialect = "duckdb";
    std::string to_dialect = "hive";
    spdlog::info("Sending TranspileSql request: ({} -> {}) {}", from_dialect, to_dialect, sql_statement);

    auto rootMenu = make_unique<Menu>("worker-cli");
    rootMenu -> Insert(
            "hello",
            [](std::ostream& out){ out << "Hello, world\n"; },
            "Print hello world" );
    rootMenu -> Insert(
            "transpile",
            [&](std::ostream& out, std::string query){
                out << "Transpiled SQL query is: " << client.TranspileSql(token, query, from_dialect, to_dialect) << std::endl;
            },
            "Call TranspileSQL Service");
    rootMenu -> Insert(
            "color",
            [](std::ostream& out){ out << "Colors ON\n"; SetColor(); },
            "Enable colors in the cli" );
    rootMenu -> Insert(
            "nocolor",
            [](std::ostream& out){ out << "Colors OFF\n"; SetNoColor(); },
            "Disable colors in the cli" );

    auto subMenu = make_unique< Menu >( "sub" );
    subMenu -> Insert(
            "hello",
            [](std::ostream& out){ out << "Hello, submenu world\n"; },
            "Print hello world in the submenu" );
    subMenu -> Insert(
            "demo",
            [](std::ostream& out){ out << "This is a sample!\n"; },
            "Print a demo string" );

    rootMenu -> Insert( std::move(subMenu) );

    Cli cli(std::move(rootMenu));

    // global exit action
    cli.ExitAction([](auto& out){
        out << "Exit worker-cli and disconnected with Pixels." << std::endl;
    });

    CliFileSession input(cli);
    input.Start();

    return 0;
}
