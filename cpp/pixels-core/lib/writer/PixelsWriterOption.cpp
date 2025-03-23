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

#include "writer/PixelsWriterOption.h"
#include <iostream>

PixelsWriterOption::PixelsWriterOption() {}

int PixelsWriterOption::getPixelsStride() const {
    return this->pixelsStride;
}

std::shared_ptr<PixelsWriterOption> PixelsWriterOption::setPixelsStride(int pixelsStride) {
    this->pixelsStride = pixelsStride;
    return shared_from_this();
}

EncodingLevel PixelsWriterOption::getEncodingLevel() const {
    return this->encodingLevel;
}

std::shared_ptr<PixelsWriterOption> PixelsWriterOption::setEncodingLevel(EncodingLevel encodingLevel) {
    this->encodingLevel = encodingLevel;
    return shared_from_this();
}

bool PixelsWriterOption::isNullsPadding() const {
    return this->nullsPadding;
}

std::shared_ptr<PixelsWriterOption> PixelsWriterOption::setNullsPadding(bool nullsPadding) {
    this->nullsPadding = nullsPadding;
    return shared_from_this();
}

ByteOrder PixelsWriterOption::getByteOrder() const {
    return byteOrder;
}

void PixelsWriterOption::setByteOrder(ByteOrder byte_order) {
    byteOrder = byte_order;
}
