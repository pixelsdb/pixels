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
#include <string>

#include "cli/cli.h"
#include "cli/clifilesession.h"
#include "db/duckdb_manager.h"
#include "grpc/transpile_sql_client.h"
#include "grpc/trino_query_client.h"
#include "spdlog/spdlog.h"
#include "yaml-cpp/yaml.h"

using namespace cli;

int main() {
  // Load configuration file
  YAML::Node config = YAML::LoadFile("config.yaml");

  // Init an in-memory DuckDB instance
  DuckDBManager db_manager(":memory:");

  // Configure disk cache
  std::string cache_dir = config["cache_dir_path"].as<std::string>();
  spdlog::info("Cached data will be stored in: {}", cache_dir);

  // Connect to pixels-server
  std::string host_addr = config["server_address"].as<std::string>();
  std::string port = config["server_port"].as<std::string>();
  TranspileSqlClient transpileSqlClient(grpc::CreateChannel(
      host_addr + ":" + port, grpc::InsecureChannelCredentials()));
  spdlog::info("Connected to server: {}", host_addr + ":" + port);

  // Configure transpile request
  std::string token = "";
  std::string from_dialect = "duckdb";
  std::string to_dialect = "hive";
  spdlog::info("Configured the SQL dialect transpile as {} -> {}", from_dialect,
               to_dialect);

  // Configure trino endpoint
  std::string trino_url = config["trino_url"].as<std::string>();
  int trino_port = config["trino_port"].as<int>();
  std::string trino_default_catalog =
      config["trino_default_catalog"].as<std::string>();
  std::string trino_default_schema =
      config["trino_default_schema"].as<std::string>();
  TrinoQueryClient trinoQueryClient(grpc::CreateChannel(
      host_addr + ":" + port, grpc::InsecureChannelCredentials()));
  spdlog::info("Configured the trino query endpoint: {}:{}/{}/{}", trino_url,
               trino_port, trino_default_catalog, trino_default_schema);

  // Start the interactive cli menu
  auto rootMenu = std::make_unique<Menu>("worker-cli");
  rootMenu->Insert(
      "hello",
      [](std::ostream& out) {
        out << "Here is CLI root menu. It provides commands for system setting "
               "and direct RPC call."
            << std::endl;
      },
      "Print the worker-cli menu functionality");
  rootMenu->Insert(
      "transpile",
      [&](std::ostream& out, std::string query) {
        try {
          std::string transpiled_result = transpileSqlClient.TranspileSql(
              token, query, from_dialect, to_dialect);
          out << "Transpiled SQL query is: " << transpiled_result << std::endl;
        } catch (const std::exception& e) {
          std::cerr << "Caught exception: " << e.what() << std::endl;
          spdlog::error("Caught transpile exception: {}", e.what());
        }
      },
      "Call TranspileSQL Service");
  rootMenu->Insert(
      "trino",
      [&](std::ostream& out, std::string query) {
        try {
          std::string query_result = trinoQueryClient.TrinoQuery(
              token, trino_url, trino_port, trino_default_catalog,
              trino_default_schema, query);
          out << "Trino query result: \n" << query_result << std::endl;
        } catch (const std::exception& e) {
          std::cerr << "Caught exception: " << e.what() << std::endl;
          spdlog::error("Caught trino query exception: {}", e.what());
        }
      },
      "Send SQL query to trino endpoint");

  rootMenu->Insert(
      "color",
      [](std::ostream& out) {
        out << "Colors ON\n";
        SetColor();
      },
      "Enable colors in the cli");
  rootMenu->Insert(
      "nocolor",
      [](std::ostream& out) {
        out << "Colors OFF\n";
        SetNoColor();
      },
      "Disable colors in the cli");

  // DuckDB CLI
  auto duckdbMenu = std::make_unique<Menu>("duckdb");
  duckdbMenu->Insert(
      "hello",
      [](std::ostream& out) {
        out << "Here is DuckDB CLI. You can run SQL queries in the local "
               "DuckDB."
            << std::endl;
      },
      "Print the DuckDB menu functionality");
  duckdbMenu->Insert(
      "run",
      [&db_manager](std::ostream& out, std::string query) {
        auto result = db_manager.executeQuery(query);
        result->Print();
      },
      "Execute single query in DuckDB instance");
  duckdbMenu->Insert(
      "import",
      [&db_manager](std::ostream& out, std::string file_path) {
        db_manager.importSqlFile(file_path);
      },
      "Import SQL file in DuckDB instance");

  rootMenu->Insert(std::move(duckdbMenu));

  Cli cli(std::move(rootMenu));

  // Global exit action
  cli.ExitAction([](auto& out) {
    out << "bye~" << std::endl;
  });

  CliFileSession input(cli);
  input.Start();

  return 0;
}
