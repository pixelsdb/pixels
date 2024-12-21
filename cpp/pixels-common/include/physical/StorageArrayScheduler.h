/*
 * Copyright 2024 PixelsDB.
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

/*
 * @author liyu
 * @create 2024-01-21
 */
//
// Created by liyu on 1/21/24.
//

#ifndef DUCKDB_STORAGEARRAYSCHEDULER_H
#define DUCKDB_STORAGEARRAYSCHEDULER_H

#include "utils/ConfigFactory.h"
#include <vector>
#include <mutex>
#include <unordered_map>

class StorageArrayScheduler
{
public:
    StorageArrayScheduler(std::vector <std::string> &files, int threadNum);

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
    std::vector <std::vector<std::string>> filesVector;
};

#endif //DUCKDB_STORAGEARRAYSCHEDULER_H
