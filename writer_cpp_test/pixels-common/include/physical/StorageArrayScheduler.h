//
// Created by liyu on 1/21/24.
//

#ifndef DUCKDB_STORAGEARRAYSCHEDULER_H
#define DUCKDB_STORAGEARRAYSCHEDULER_H

#include "utils/ConfigFactory.h"
#include <vector>
#include <mutex>
#include <unordered_map>

class StorageArrayScheduler {
public:
    StorageArrayScheduler(std::vector<std::string>& files, int threadNum);
    int acquireDeviceId();
    int getDeviceSum();

    std::string getFileName(int deviceID, int fileID);
    uint64_t getFileSum(int deviceID);
    int getMaxFileSum();
    int getBatchID(int deviceID, int fileID);
private:
    std::mutex m;
    int currentDeviceID;
    int devicesNum;
    std::vector<std::vector<std::string>> filesVector;
};

#endif //DUCKDB_STORAGEARRAYSCHEDULER_H
