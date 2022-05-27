//
// Created by hank on 09/11/2020.
//

#ifndef STORAGE_MANAGER_CONSTANTS_H
#define STORAGE_MANAGER_CONSTANTS_H

#include <string>

namespace stm {

/**
 * Constants that are used for STM.
 */
class Constants {
public:
  static std::string currentContainerName;
  static std::uint64_t CATALOG_MQ_FILE_SIZE;
  static std::uint64_t CATALOG_MESSAGE_SIZE;
  static std::string CATALOG_MAIN_REQ_MQ;
  static std::string CATALOG_SUB_RES_MQ_BASE;
  static std::string CATALOG_GRPC_SERVER_HOST;
  static std::string CATALOG_GRPC_SERVER_PORT;
  static int CATALOG_SERVER_NUM_IPC_THREADS;

  // Memory tier
  static std::uint64_t STORAGE_MEMORY_TIER_SIZE; // 32GB memory tier
  static std::uint64_t STORAGE_MEMORY_PAGE_SIZE; // 256KB page size
  static std::uint64_t
      STORAGE_MEMORY_BLOCK_SIZE_NUM; // 18 different block sizes, 32GB max
  static std::string STORAGE_MEMORY_TIER_PATH;

  // SSD tier
  static std::uint64_t STORAGE_SSD_TIER_SIZE; // 128GB hdd tier
  static std::uint64_t STORAGE_SSD_PAGE_SIZE; // 1MB page size
  static std::uint64_t
      STORAGE_SSD_BLOCK_SIZE_NUM; // 18 different block sizes, 128GB max
  static std::string STORAGE_SSD_TIER_PATH; // create it with 'fallocate -l
                                            // 137438953472 [path]' in
  // advance

  // HDD tier
  static std::uint64_t STORAGE_HDD_TIER_SIZE; // 512GB hdd tier
  static std::uint64_t STORAGE_HDD_PAGE_SIZE; // 4MB page size
  static std::uint64_t
      STORAGE_HDD_BLOCK_SIZE_NUM; // 18 different block sizes, 512GB max
  static std::string STORAGE_HDD_TIER_PATH; // create it with 'fallocate -l
                                            // 549755813888 [path]' in
  // advance

  static std::string TIERING_POLICY_FOR_MEMORY;
  static std::string TIERING_POLICY_FOR_SSD;
  static std::string TIERING_POLICY_FOR_HDD;

  // XGBOOST model
  static std::string XGB_MODEL_PATH;
  static int XGB_MAX_OBJECT_MB;
  static int XGB_MAX_TIME_DELTA_MIN;
};
} // namespace stm

#endif // STORAGE_MANAGER_CONSTANTS_H
