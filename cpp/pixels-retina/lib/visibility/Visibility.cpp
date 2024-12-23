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
#include <iostream>

Visibility::Visibility() : allValue(0) {
    std::cout << "call construction function\n";
    std::memset(this->deleteBitmap1, 0, sizeof(uint64_t) * 4);
    std::memset(this->deleteBitmap2, 0, sizeof(uint64_t) * 4);
}

Visibility::~Visibility() {
    std::cout << "call destroy function\n";
}

std::vector<uint64_t> Visibility::getReadableBitmap(int timestamp) {
    std::cout << "call getReadableBitmap\n";
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<uint64_t> readableBitmap(4, 0xFFFFFFFFFFFFFFFF);

    if (this->allValue == 1) {
        readableBitmap.assign(4, 0);
    } else if (allValue == -1) {
        for(int i = 0; i < 4; ++i) {
            readableBitmap[i] = ~(deleteBitmap1[i] | deleteBitmap2[i]);
        }
    }

    // TODO: judge delete timestamp
    return readableBitmap;
}

//void DeleteTrackerNative::markRowPreparedDelete(int timestamp, int rowId) {
//    std::lock_guard<std::mutex> lock(mutex_);
//    if (GET_BITMAP_BIT(deleteBitmap1, rowId) && GET_BITMAP_BIT(deleteBitmap2, rowId)) {
//        throw std::runtime_error("this row is already marked for delete");
//    }
//    SET_BITMAP_BIT(deleteBitmap1, rowId);
//    allValue = -1;
//}
//
//void DeleteTrackerNative::markRowDeleted(int rowId) {
//    std::lock_guard<std::mutex> lock(mutex_);
//    SET_BITMAP_BIT(deleteBitmap1, rowId);
//    SET_BITMAP_BIT(deleteBitmap2, rowId);
//    allValue = -1;
//}

void Visibility::deleteRow(int rowId, int timestamp) {
    std::cout << "call deleteRow\n";
    std::lock_guard<std::mutex> lock(mutex_);

    // set delete status to 10
    SET_BITMAP_BIT(deleteBitmap1, rowId);

    //TODO: add delete timestamp
}
