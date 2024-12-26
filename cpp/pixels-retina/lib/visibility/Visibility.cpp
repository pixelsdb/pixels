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

#include "visibility/Visibility.h"
#include <cstring>
#include <stdexcept>

Visibility::Visibility() : allValue(0) {
    this->intendDeleteBitmap = new std::uint64_t[BITMAP_ARRAY_SIZE]();
    this->actualDeleteBitmap = new std::uint64_t[BITMAP_ARRAY_SIZE]();
}

Visibility::~Visibility() {
    delete[] intendDeleteBitmap;
    delete[] actualDeleteBitmap;
}

void Visibility::getVisibilityBitmap(std::uint64_t epochTs, std::uint64_t *visibilityBitmap) {
    std::lock_guard<std::mutex> lock(mutex_);

    std::memset(visibilityBitmap, 0, sizeof(std::uint64_t) * BITMAP_ARRAY_SIZE);

    int epochIndex = findEpoch(epochTs);
    std::size_t patchStart = epochArr[epochIndex].patchStartIndex;
    if (patchStart + CHECKPOINT_SIZE > patchArr.size()) {
        throw std::logic_error("Invalid state: patchStartIndex out of bound");
    }
    std::memcpy(visibilityBitmap, &patchArr[patchStart], CHECKPOINT_SIZE);

    std::size_t patchPos = patchStart + 32;
    std::size_t patchEnd = epochArr[epochIndex].patchEndIndex;
    if (patchPos <= patchEnd && patchEnd <= patchArr.size()) {
        for (; patchPos < patchEnd; ++patchPos) {
            SET_BITMAP_BIT(visibilityBitmap, patchArr[patchPos]);
        }
    }

}

void Visibility::deleteRecord(int rowId, std::uint64_t epochTs) {
    std::lock_guard<std::mutex> lock(mutex_);
    rowId %= RECORDS_PER_VISIBILITY;
    if (GET_BITMAP_BIT(intendDeleteBitmap, rowId) == 0 &&
            GET_BITMAP_BIT(actualDeleteBitmap, rowId) == 0) {
        SET_BITMAP_BIT(intendDeleteBitmap, rowId);
        int epochIndex = findEpoch(epochTs);
        patchArr.push_back(static_cast<std::uint8_t>(rowId));
        epochArr[epochIndex].patchEndIndex += 1;
    } else {
        throw std::logic_error("Invalid state: impossible state");
    }

    for (int i = 0; i < BITMAP_ARRAY_SIZE; ++i) {
        if (actualDeleteBitmap[i] != UINT64_MAX) {
            allValue = -1;
            return;
        }
    }
    allValue = 1; // all records are deleted
}

void Visibility::createNewEpoch(std::uint64_t epochTs) {
    std::lock_guard<std::mutex> lock(mutex_);

    EpochEntry newEpoch{};
    newEpoch.timestamp = epochTs;
    newEpoch.patchStartIndex = patchArr.size();

    std::uint8_t checkpointBuf[CHECKPOINT_SIZE];
    std::memcpy(checkpointBuf, intendDeleteBitmap, CHECKPOINT_SIZE);

    for (int i = 0; i < CHECKPOINT_SIZE; ++i) {
        patchArr.push_back(checkpointBuf[i]);
    }

    newEpoch.patchEndIndex = patchArr.size();
    epochArr.push_back(newEpoch);
}

std::size_t Visibility::findEpoch(std::uint64_t epochTs) const {
    std::size_t l = 0, r = epochArr.size() - 1;
    while (l <= r) {
        std::size_t mid = l + (r - l) / 2;
        if (epochArr[mid].timestamp <= epochTs) {
            l = mid + 1;
        } else {
            r = mid - 1;
        }
    }
    if (r == -1) {
        throw std::logic_error("Invalid state: epoch not found");
    }
    return r;
}

void Visibility::cleanEpochArrAndPatchArr(std::uint64_t cleanUpToEpochTs) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::size_t removeCount = 0;
    while (removeCount < epochArr.size() &&
           epochArr[removeCount].timestamp <= cleanUpToEpochTs) {
        removeCount++;
    }

    if (removeCount == 0) {
        return;
    } else if (removeCount >= epochArr.size()) {
        throw std::logic_error("Invalid state: cannot remove all epochs");
    } else {
        epochArr.erase(epochArr.begin(), epochArr.begin() + removeCount);
    }

    std::size_t freePatchUpTo = epochArr[removeCount].patchStartIndex;
    patchArr.erase(patchArr.begin(), patchArr.begin() + freePatchUpTo);
    for (auto &epoch : epochArr) {
        epoch.patchStartIndex -= freePatchUpTo;
        epoch.patchEndIndex -= freePatchUpTo;
    }
}