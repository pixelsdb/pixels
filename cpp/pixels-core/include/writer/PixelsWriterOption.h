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

//
// Created by gengdy on 24-11-25.
//

#ifndef PIXELS_PIXELSWRITEROPTION_H
#define PIXELS_PIXELSWRITEROPTION_H

#include "encoding/EncodingLevel.h"
#include <memory>
#include "physical/natives/ByteOrder.h"

class PixelsWriterOption : public std::enable_shared_from_this<PixelsWriterOption> {
public:
    PixelsWriterOption();
    int getPixelsStride() const;
    std::shared_ptr<PixelsWriterOption> setPixelsStride(int pixelsStride);
    EncodingLevel getEncodingLevel() const;
    std::shared_ptr<PixelsWriterOption> setEncodingLevel(EncodingLevel encodingLevel);
    bool isNullsPadding() const;
    std::shared_ptr<PixelsWriterOption> setNullsPadding(bool nullsPadding);
private:
    int pixelsStride;
    EncodingLevel encodingLevel;
    /**
     * Whether nulls positions in column are padded by arbitrary values and occupy storage and memory space.
     */
    bool nullsPadding;
    ByteOrder byteOrder{ByteOrder::PIXELS_LITTLE_ENDIAN};
public:
    ByteOrder getByteOrder() const;
    void setByteOrder(ByteOrder byte_order);
};
#endif //PIXELS_PIXELSWRITEROPTION_H
