//
// Created by liyu on 1/21/24.
//
#include "physical/StorageArrayScheduler.h"


StorageArrayScheduler::StorageArrayScheduler(std::vector<std::string> &files, int threadNum) {
    std::unordered_map<std::string, int> device2id;
    int storageDepth = std::stoi(ConfigFactory::Instance().getProperty("storage.directory.depth"));
    filesVector.clear();

    for (auto& file: files) {
        std::string deviceName;
        std::string tmp = file.substr(1);
        for(int i = 0; i < storageDepth; i++) {
            if (tmp.find('/') != std::string::npos) {
                auto loc = tmp.find('/');
                deviceName += tmp.substr(0,loc);
                tmp = tmp.substr(loc);
            } else {
                throw InvalidArgumentException("StorageArrayScheduler::initialize: wrong storage depth. ");
            }
        }
        // The following code makes sure that one thread can also process multiple devices
        if (!device2id.count(deviceName)) {
            device2id[deviceName] = (int)device2id.size() % threadNum;
        }
        int id = device2id[deviceName];
        if (id >= filesVector.size()) {
            filesVector.emplace_back(std::vector<std::string>{});
        }
        filesVector[id].emplace_back(file);
    }

    devicesNum = (int)filesVector.size();
    if (files.size() > threadNum && devicesNum % threadNum != 0 && threadNum % devicesNum != 0) {
        throw InvalidArgumentException("StorageArrayScheduler::initialize: "
                                       "if multiple devices are used, make sure "
                                       "the thread count is divisible by device num or"
                                       "the device num is divisible by the thread count. "
                                       "Now the thread count is " + std::to_string(threadNum) +
                                       " , and the storage device num is " + std::to_string(devicesNum) +
                                       ". Otherwise the load balancing issue occurs. ");
    }
    currentDeviceID = 0;
}

int StorageArrayScheduler::acquireDeviceId() {
    m.lock();
    int deviceId = currentDeviceID;
    currentDeviceID = (currentDeviceID + 1) % devicesNum;
    m.unlock();
    return deviceId;
}

int StorageArrayScheduler::getDeviceSum() {
    return devicesNum;
}
uint64_t StorageArrayScheduler::getFileSum(int deviceID) {
    return filesVector[deviceID].size();
}

std::string StorageArrayScheduler::getFileName(int deviceID, int fileID) {
    return filesVector.at(deviceID).at(fileID);
}

int StorageArrayScheduler::getMaxFileSum() {
    int result = 0;
    for (auto &files: filesVector) {
        result = std::max(result, (int)files.size());
    }
    return result;
}

int StorageArrayScheduler::getBatchID(int deviceID, int fileID) {
    int result = 0;
    for(int i = 0; i < deviceID; i++) {
        result += (int)filesVector[i].size();
    }
    result += fileID;
    return result;
}

