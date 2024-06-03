//
// Created by bian on 7/4/21.
//

#include "mq/constants.h"

using namespace stm;

std::string Constants::currentContainerName = "test";
std::uint64_t Constants::CATALOG_MQ_FILE_SIZE = (1024 * 1024);
std::uint64_t Constants::CATALOG_MESSAGE_SIZE = 256;
std::string Constants::CATALOG_MAIN_REQ_MQ = "/dev/shm/main_mq";
std::string Constants::CATALOG_SUB_RES_MQ_BASE = "/dev/shm/";
std::string Constants::CATALOG_GRPC_SERVER_HOST = "0.0.0.0";
std::string Constants::CATALOG_GRPC_SERVER_PORT = "50051";
int Constants::CATALOG_SERVER_NUM_IPC_THREADS = 4;

// Memory tier
std::uint64_t Constants::STORAGE_MEMORY_TIER_SIZE =
    (1024L * 1024L * 1024L * 32L); // 32GB memory tier
std::uint64_t Constants::STORAGE_MEMORY_PAGE_SIZE =
    (256 * 1024); // 256KB page size
std::uint64_t Constants::STORAGE_MEMORY_BLOCK_SIZE_NUM =
    18; // 18 different block sizes, 32GB max
std::string Constants::STORAGE_MEMORY_TIER_PATH = "/dev/shm/mem";

// SSD tier
std::uint64_t Constants::STORAGE_SSD_TIER_SIZE =
    (1024L * 1024L * 1024L * 128L); // 128GB hdd tier
std::uint64_t Constants::STORAGE_SSD_PAGE_SIZE =
    (1 * 1024 * 1024); // 1MB page size
std::uint64_t Constants::STORAGE_SSD_BLOCK_SIZE_NUM =
    18; // 18 different block sizes, 128GB max
std::string Constants::STORAGE_SSD_TIER_PATH =
    "/scratch/sdl/ssd"; // create it with 'fallocate -l 137438953472 [path]' in
// advance

// HDD tier
std::uint64_t Constants::STORAGE_HDD_TIER_SIZE =
    (1024L * 1024L * 1024L * 512L); // 512GB hdd tier
std::uint64_t Constants::STORAGE_HDD_PAGE_SIZE =
    (4 * 1024 * 1024); // 4MB page size
std::uint64_t Constants::STORAGE_HDD_BLOCK_SIZE_NUM =
    18; // 18 different block sizes, 512GB max
std::string Constants::STORAGE_HDD_TIER_PATH =
    "/scratch/sdl/hdd"; // create it with 'fallocate -l 549755813888 [path]' in
// advance

std::string Constants::TIERING_POLICY_FOR_MEMORY = "LRU";
std::string Constants::TIERING_POLICY_FOR_SSD = "LRU";
std::string Constants::TIERING_POLICY_FOR_HDD = "LRU";

// XGBOOST model
std::string Constants::XGB_MODEL_PATH = "this is a path";
int Constants::XGB_MAX_OBJECT_MB = 100000;
int Constants::XGB_MAX_TIME_DELTA_MIN = 24 * 60;
