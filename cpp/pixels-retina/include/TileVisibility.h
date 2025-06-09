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
#ifndef PIXELS_RETINA_TILE_VISIBILITY_H
#define PIXELS_RETINA_TILE_VISIBILITY_H

#ifndef SET_BITMAP_BIT
#define SET_BITMAP_BIT(bitmap, rowId) \
    ((bitmap)[(rowId) / 64] |= (1ULL << ((rowId) % 64)))
#endif

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>

inline uint64_t makeDeleteIndex(uint8_t rowId, uint64_t ts) {
    return (static_cast<uint64_t>(rowId) << 56 | (ts & 0x00FFFFFFFFFFFFFFULL));
}

inline uint8_t extractRowId(uint64_t raw) {
    return static_cast<uint8_t>(raw >> 56);
}

inline uint64_t extractTimestamp(uint64_t raw) {
    return (raw & 0x00FFFFFFFFFFFFFFULL);
}

struct DeleteIndexBlock {
    static constexpr size_t BLOCK_CAPACITY = 8;
    uint64_t items[BLOCK_CAPACITY] = {0};
    std::atomic<DeleteIndexBlock *> next{nullptr};
};

class TileVisibility {
  public:
    TileVisibility();
    TileVisibility(uint64_t ts, const uint64_t bitmap[4]);
    ~TileVisibility();
    void deleteTileRecord(uint8_t rowId, uint64_t ts);
    void getTileVisibilityBitmap(uint64_t ts, uint64_t outBitmap[4]) const;
    void collectTileGarbage(uint64_t ts);

  private:
    TileVisibility(const TileVisibility &) = delete;
    TileVisibility &operator=(const TileVisibility &) = delete;

    uint64_t baseBitmap[4];
    uint64_t baseTimestamp;
    std::atomic<DeleteIndexBlock *> head;
    std::atomic<DeleteIndexBlock *> tail;
    std::atomic<size_t> tailUsed;
};

#endif // PIXELS_RETINA_TILE_VISIBILITY_H
