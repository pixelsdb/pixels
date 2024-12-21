/*
 * Copyright 2023 PixelsDB.
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
 * @create 2023-03-20
 */
#ifndef PIXELS_DECODER_H
#define PIXELS_DECODER_H

#include <memory>
#include <iostream>
#include "physical/natives/ByteBuffer.h"

class Decoder
{
public:
    virtual void close() = 0;

    virtual long next() = 0;

    virtual bool hasNext() = 0;
};
#endif //PIXELS_DECODER_H
