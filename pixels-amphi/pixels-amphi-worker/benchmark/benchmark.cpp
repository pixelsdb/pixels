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
#include <chrono>
#include <sstream>
#include <iomanip>
#include <unistd.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <stdexcept>
#include <fstream>
#include <map>
#include <vector>

#include "yaml-cpp/yaml.h"
#include "nlohmann/json.hpp"
#include "spdlog/spdlog.h"
#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/sinks/stdout_sinks.h"

#include "grpc/coordinate_query_client.h"
#include "grpc/transpile_sql_client.h"
#include "grpc/trino_query_client.h"
#include "grpc/metadata_client.h"
#include "db/duckdb_manager.h"
#include "metadata.pb.h"
#include "metadata.grpc.pb.h"

std::string getHostAddress();
YAML::Node loadConfig(const std::string& filename);
std::map<std::string, std::vector<std::string>> loadCachePlan(const std::string& filename);
std::vector<std::string> readWorkloadQueries(const std::string& filename);
void reportCachePath(MetadataClient& metadata_client,
                     std::string schema_name,
                     metadata::proto::Peer registered_peer,
                     std::string cache_data_path,
                     std::map<std::string, std::vector<std::string>> cache_plan);
std::string getTimestamp();

std::shared_ptr<spdlog::logger> logger = spdlog::stdout_logger_mt("console");;

/* Run benchmark with two configuration paths, for worker and benchmark respectively. */
int main(int argc, char** argv) {
    // Load configuration
    if (argc != 3) {
        std::cerr << "Please provide exactly two configuration file paths as arguments" << std::endl;
        return 1;
    }
    YAML::Node worker_config = loadConfig(argv[1]);
    YAML::Node benchmark_config = loadConfig(argv[2]);

    std::string exp_name = benchmark_config["exp_name"].as<std::string>();
    std::string schema_name = benchmark_config["schema_name"].as<std::string>();
    std::string benchmark_dir_path = benchmark_config["benchmark_path"].as<std::string>();
    std::string cache_data_path = benchmark_config["cache_data_path"].as<std::string>();
    std::string log_filename = benchmark_dir_path + "logs/" + exp_name + "_" + getTimestamp() + ".log";
    // logger = spdlog::basic_logger_mt("basic_logger", log_filename);
    logger->info("Running the benchmark for experiment: " + exp_name);
    logger->info("Data cached in directory: " + cache_data_path);

    // Output direct results (time cost for query execution)
    std::string results_filename = benchmark_dir_path + "results/" + exp_name + "_" + getTimestamp() + ".txt";
    std::ofstream results_file(results_filename);
    if (!results_file) {
        std::cerr << "Failed to open results file: " << results_filename << std::endl;
        return 1;
    }
    logger->info("Writing all results to file: " + results_filename);

    // Worker may directly request to metadata server, but no need to connect to the trino endpoint
    std::string pixels_server_addr = worker_config["server_address"].as<std::string>();
    int pixels_server_port = worker_config["server_port"].as<int>();
    int metadata_port = worker_config["metadata_service_port"].as<int>();
    MetadataClient metadata_client(grpc::CreateChannel(pixels_server_addr + ":" + std::to_string(metadata_port), grpc::InsecureChannelCredentials()));
    logger->info("Set pixels server endpoint: " + pixels_server_addr + ":" + std::to_string(pixels_server_port));
    logger->info("Set metadata server endpoint: " + pixels_server_addr + ":" + std::to_string(metadata_port)); // assumed on the same server for simplicity

    // Register the peer
    std::string worker_name = worker_config["worker_name"].as<std::string>();
    std::string worker_location = worker_config["worker_location"].as<std::string>();
    int worker_port = worker_config["worker_port"].as<int>();

    metadata::proto::Peer peer;
    peer.set_name(worker_name);
    peer.set_location(worker_location);
    peer.set_host(getHostAddress());
    peer.set_port(worker_port);
    peer.set_storagescheme("file");
    metadata::proto::CreatePeerRequest create_peer_request;
    create_peer_request.mutable_header()->set_token("");
    create_peer_request.mutable_peer()->CopyFrom(peer);
    metadata::proto::CreatePeerResponse create_peer_response = metadata_client.CreatePeer(create_peer_request);

    if (create_peer_response.header().errorcode() == 0) {
        logger->info("Register the worker catalog: " + worker_name);
    } else {
        logger->error("CreatePeer failed with error code: {} and error message: {}",
                      create_peer_response.header().errorcode(),
                      create_peer_response.header().errormsg());
    }
    metadata::proto::GetPeerRequest get_peer_request;
    get_peer_request.mutable_header()->set_token("");
    get_peer_request.set_name(worker_name);
    metadata::proto::Peer registered_peer = metadata_client.GetPeer(get_peer_request).peer();

    // Report peer paths (assumed that we have cached the data)
    std::string cache_plan_path = benchmark_dir_path + benchmark_config["cache_plan_path"].as<std::string>();
    std::map<std::string, std::vector<std::string>> cache_plan = loadCachePlan(cache_plan_path);
    reportCachePath(metadata_client, schema_name, registered_peer, cache_data_path, cache_plan);

    // Create table on the cached parquet folders with DuckDB instance
    DuckDBManager db_manager(":memory:");
    std::string init_sql_path = benchmark_dir_path + benchmark_config["init_sql_path"].as<std::string>();
    db_manager.importSqlFile(init_sql_path);
    logger->info("DuckDB instance created view given in SQL: " + init_sql_path);

    // Iterate the workload sql queries, send coordinator query request
    // If no error and in-cloud false, execute the query with DuckDB
    std::string workload_path = benchmark_dir_path + benchmark_config["workload_path"].as<std::string>();
    std::vector<std::string> workload = readWorkloadQueries(workload_path);
    CoordinateQueryClient coordinator_client(grpc::CreateChannel(pixels_server_addr + ":" + std::to_string(pixels_server_port), grpc::InsecureChannelCredentials()));
    for (const std::string& query : workload) {
	logger->info("Sent query " + query  + " to Coordinator");
        auto coordinator_start_time = std::chrono::high_resolution_clock::now();
        amphi::proto::CoordinateQueryResponse coordinator_response = coordinator_client.CoordinateQuery("", worker_name, schema_name, query);
        auto coordinator_end_time = std::chrono::high_resolution_clock::now();
        auto coordinator_elapsed_time = std::chrono::duration_cast<std::chrono::microseconds>(coordinator_end_time - coordinator_start_time).count();

        // Adaptive decision: already get in-cloud result or need to execute on-premises
        bool inCloud = coordinator_response.incloud();
        if (inCloud) {
            logger->info("Query In-cloud execution: \n" + coordinator_response.queryresult());
            results_file << "cloud" << std::endl;;
            results_file << coordinator_elapsed_time << std::endl;;
        } else {
            auto engine_start_time = std::chrono::high_resolution_clock::now();
            auto result = db_manager.executeQuery(query);
            auto engine_end_time = std::chrono::high_resolution_clock::now();
            auto engine_elapsed_time = std::chrono::duration_cast<std::chrono::microseconds>(engine_end_time - engine_start_time).count();
            std::streambuf* original_buf = std::cout.rdbuf();
            std::ostringstream str_cout;
            std::cout.rdbuf(str_cout.rdbuf());
            result->Print();
            std::cout.rdbuf(original_buf);
            logger->info("Query On-premises execution: \n" + str_cout.str());
            results_file << "worker" << std::endl;;
            results_file << engine_elapsed_time << std::endl;;
        }
    }

    // To keep the process stateless, delete catalog information
    // Note: only need to delete peer, its relevant peer paths will be deleted in cascade
    metadata::proto::DeletePeerRequest delete_peer_request;
    delete_peer_request.mutable_header()->set_token("");
    delete_peer_request.set_name(worker_name);
    metadata::proto::DeletePeerResponse delete_peer_response = metadata_client.DeletePeer(delete_peer_request);
    if (delete_peer_response.header().errorcode() == 0) {
        logger->info("Deleted the worker catalog: " + worker_name);
    } else {
        logger->error("DeletePeer failed with error code: {} and error message: {}",
                      delete_peer_response.header().errorcode(),
                      delete_peer_response.header().errormsg());
    }

    results_file.close();
    return 0;
}

std::string getHostAddress() {
    char hostname[1024];
    gethostname(hostname, 1024);

    struct hostent* h;
    h = gethostbyname(hostname);
    if (h == NULL) {
        throw std::runtime_error("Unknown host");
    }

    struct in_addr** addr_list = (struct in_addr**)h->h_addr_list;
    for(int i = 0; addr_list[i] != NULL; i++) {
        return inet_ntoa(*addr_list[i]);
    }

    throw std::runtime_error("No host address found");
}

YAML::Node loadConfig(const std::string& filename) {
    try {
        YAML::Node config = YAML::LoadFile(filename);
        return config;
    } catch (const YAML::BadFile& e) {
        std::cerr << "Failed to open file: " << filename << std::endl;
    } catch (const YAML::ParserException& e) {
        std::cerr << "Failed to parse file: " << filename << " Error: " << e.what() << std::endl;
    }
    return YAML::Node();
}

std::map<std::string, std::vector<std::string>> loadCachePlan(const std::string& filename) {
    std::ifstream file(filename);
    nlohmann::json json_data;
    file >> json_data;
    return json_data.get<std::map<std::string, std::vector<std::string>>>();
}

std::vector<std::string> readWorkloadQueries(const std::string& filename) {
    std::ifstream file(filename);
    std::string str;
    std::vector<std::string> workload;

    if(!file.is_open()) {
        std::cout << "Failed to open file" << std::endl;
        return workload;
    }

    while(std::getline(file, str)) {
        workload.push_back(str);
    }

    file.close();
    return workload;
}

void reportCachePath(MetadataClient& metadata_client,
                     std::string schema_name,
                     metadata::proto::Peer registered_peer,
                     std::string cache_data_path,
                     std::map<std::string, std::vector<std::string>> cache_plan) {

    // Iterate through the cache plan
    for(const auto& pair : cache_plan) {
        const std::string& table = pair.first;
        const std::vector<std::string>& column_name = pair.second;
        if (column_name.size() == 0) { // skip empty cached table
            continue;
        }

        // Get column metadata of the table
        metadata::proto::GetColumnsRequest getcolumns_request;
        getcolumns_request.set_schemaname(schema_name);
        getcolumns_request.set_tablename(table);
        metadata::proto::GetColumnsResponse getcolumns_response =
                metadata_client.GetColumns(getcolumns_request);

        // Only preserve the cached columns
        std::vector<metadata::proto::Column> cached_columns;
        for (const auto& column : getcolumns_response.columns()) {
            if (std::find(column_name.begin(), column_name.end(), column.name()) != column_name.end()) {
                cached_columns.push_back(column);
            }
        }

        // Get the ordered directory path from the latest layout
        metadata::proto::GetLayoutRequest getlayout_request;
        getlayout_request.set_schemaname(schema_name);
        getlayout_request.set_tablename(table);
        getlayout_request.set_layoutversion(-1); // latest version
        metadata::proto::Layout layout = metadata_client.GetLayout(getlayout_request).layout();
        metadata::proto::Path path = layout.orderedpaths().Get(0);

        // Request CreatePeerPath
        metadata::proto::CreatePeerPathRequest createpeerpath_request;
        createpeerpath_request.mutable_header()->set_token("");
        metadata::proto::PeerPath peer_path;
        peer_path.set_uri(cache_data_path + table);
        for (const metadata::proto::Column& column : cached_columns) {
            peer_path.add_columns()->CopyFrom(column);
        }
        peer_path.set_pathid(path.id());
        peer_path.set_peerid(registered_peer.id());
	createpeerpath_request.mutable_peerpath()->CopyFrom(peer_path);
        metadata::proto::CreatePeerPathResponse createpeerpath_response =
                metadata_client.CreatePeerPath(createpeerpath_request);

        if (createpeerpath_response.header().errorcode() == 0) {
            logger->info("Created peer path for table: " + table);
        } else {
            logger->error("CreatePeerPath failed with error code: {} and error message: {}",
                          createpeerpath_response.header().errorcode(),
                          createpeerpath_response.header().errormsg());
        }
    }
    return;
}

std::string getTimestamp() {
    auto now = std::chrono::system_clock::now();
    auto in_time_t = std::chrono::system_clock::to_time_t(now);

    std::stringstream ss;
    ss << std::put_time(std::localtime(&in_time_t), "%Y%m%d_%H%M");
    return ss.str();
}
