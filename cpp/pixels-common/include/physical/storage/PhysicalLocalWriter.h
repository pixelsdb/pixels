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
 * @author gengdy
 * @create 2024-11-25
 */
#ifndef PIXELS_PHYSICALLOCALWRITER_H
#define PIXELS_PHYSICALLOCALWRITER_H

#include "physical/PhysicalWriter.h"
#include "physical/storage/LocalFS.h"
#include "physical/natives/ByteBuffer.h"
#include <fstream>

class PhysicalLocalWriter : public PhysicalWriter
{
public:
    PhysicalLocalWriter(const std::string &path, bool overwrite);

    std::int64_t prepare(int length) override;

    std::int64_t append(const uint8_t *buffer, int offset, int length) override;

    std::int64_t append(std::shared_ptr <ByteBuffer> byteBuffer) override;

    void close() override;

    void flush() override;

    std::string getPath() const override;

    int getBufferSize() const override;

private:
    std::shared_ptr <LocalFS> localFS;
    std::string path;
    std::int64_t position;
    std::ofstream rawWriter;
};
#endif //PIXELS_PHYSICALLOCALWRITER_H
