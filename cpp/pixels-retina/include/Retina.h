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
#ifndef VISIBILITYUTIL_H
#define VISIBILITYUTIL_H

#include "Visibility.h"
#include <memory>
#include <atomic>

class Retina {
public:
    explicit Retina(uint64_t rgRecordNum);
    ~Retina();

    void deleteRecord(uint64_t rowId, uint64_t timestamp);
    uint64_t* getVisibilityBitmap(uint64_t timestamp);

    void garbageCollect(uint64_t timestamp);

private:
    static constexpr uint32_t VISIBILITY_RECORD_CAPACITY = 256;
    static constexpr uint32_t MAX_ACCESS_COUNT = 0x007FFFFF;
    static constexpr uint32_t GC_MASK = 0xFF000000;
    static constexpr uint32_t ACCESS_MASK = 0x00FFFFFF;
    static constexpr uint32_t ACCESS_INC = 0x00000001;
    static constexpr uint32_t BITMAP_SIZE_PER_VISIBILITY = 4;
    static constexpr uint32_t RG_READ_LEASE_MS = 100;

    Visibility* visibilities;
    const uint64_t numVisibilities;
    std::atomic<uint32_t> flag; // high 1 byte is the gc flag, low 3 bytes are the access count

    Visibility* getVisibility(uint64_t rowId) const;
    void beginAccess();
    void endAccess();
};

#endif //VISIBILITYUTIL_H
