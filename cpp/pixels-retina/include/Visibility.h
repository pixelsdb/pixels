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
#ifndef PIXELS_RETINA_VISIBILITY_H
#define PIXELS_RETINA_VISIBILITY_H

#include <cstdint>
#include <cstddef>

class Visibility {
public:
    Visibility();
    ~Visibility();

    void setAllValue(int8_t value);
    int8_t getAllValue() const;

    void createNewEpoch(uint64_t epochTs);
    void deleteRecord(std::size_t rowId, uint64_t epochTs);
    void getVisibilityBitmap(uint64_t epochTs, uint64_t outBitmap[4]) const;
    void cleanOldEpochs(uint64_t cutoffTs);

private:
    struct Impl;
    Impl* m_impl;
    Visibility(const Visibility&) = delete;
    Visibility& operator=(const Visibility&) = delete;

    static const std::size_t CHECKPOINT_SIZE = 32;
};

#endif //PIXELS_RETINA_VISIBILITY_H
