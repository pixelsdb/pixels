/*
 * Copyright 2025 PixelsDB.
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

#include "Visibility.h"
#include "EpochList.h"
#include "PatchArr.h"

#include <atomic>
#include <cstring>
#include <stdexcept>
#include <iostream>

#ifndef SET_BITMAP_BIT
#define SET_BITMAP_BIT(bitmap, rowId) \
    ((bitmap)[(rowId) / 64] |= (1ULL << ((rowId) % 64)))
#endif

struct Visibility::Impl {
    std::atomic<int8_t> allValue;
    EpochList epochList;
    PatchArr patchArr;
    Impl() {
        allValue.store(0, std::memory_order_relaxed);
    }
};

Visibility::Visibility() {
    m_impl = new Impl();
}

Visibility::~Visibility() {
    delete m_impl;
    m_impl = nullptr;
}

void Visibility::setAllValue(int8_t value) {
    m_impl->allValue.store(val, std::memory_order_relaxed);
}

int8_t Visibility::getAllValue() const {
    return m_impl->allValue.load(std::memory_order_relaxed);
}

void Visibility::createNewEpoch(uint64_t epochTs) {
    /**
     * TODO: wrong implement, no need to write checkpoint if not full
     */
    unsigned char ckp[CHECKPOINT_SIZE];
    std::memset(ckp, 0, 32);
    uint8_t chunkIndex = m_impl->patchArr.writeCheckPoint(ckp);
    m_impl->epochList.addEpoch(epochTs, chunkIndex, 32);
}

void Visibility::deleteRecord(std::size_t rowId, uint64_t epochTs) {
    EpochInfo* info = m_impl->epochList.findEpoch(epochTs);
    if (!info) {
        std::cerr << "[deleteRecord] Epoch not found: ts=" << epochTs << "\n";
        return;
    }

    uint8_t patchIndex = 0;
    uint16_t patchOffset = 0;
    m_impl->patchArr.appendDeleteIndex((unsigned char) (rowId & 0xFF), patchIndex, patchOffset);

    info->patchIndex = patchIndex;
    info->patchOffset = patchOffset;
}

void Visibility::getVisibilityBitmap(uint64_t epochTs, uint64_t *outBitmap) const {
    int8_t av = getAllValue();
    if (av == 0) {
        std::memset(outBitmap, 0, 4 * sizeof(uint64_t));
        return;
    } else if (av == 1) {
        std::memset(outBitmap, 0xFF, 4 * sizeof(uint64_t));
        return;
    }

    EpochInfo* info = m_impl->epochList.findEpoch(epochTs);
    if (!info) {
        std::memset(outBitmap, 0, 4 * sizeof(uint64_t));
        return;
    }

    std::memset(outBitmap, 0, 4 * sizeof(uint64_t));
    if (info->patchOffset < 32) {
        return;
    }

    unsigned char buffer[64];
    std::memset(buffer, 0, 64);
    m_impl->patchArr.readData(info->patchIndex, info->patchOffset, buffer, 64);

    uint16_t pos = 32;
    while (pos < info->patchOffset) {
        unsigned char rowId = buffer[pos];
        SET_BITMAP_BIT(outBitmap, rowId);
        pos++;
    }
}

void Visibility::cleanOldEpochs(uint64_t cutoffTs) {
    m_impl->epochList.cleanOldEpochs(cutoffTs);
}