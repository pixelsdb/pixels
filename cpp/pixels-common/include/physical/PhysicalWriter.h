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
#ifndef PIXELS_PHYSICALWRITER_H
#define PIXELS_PHYSICALWRITER_H

#include <cstdint>
#include <string>
#include "physical/natives/ByteBuffer.h"


class PhysicalWriter {
public:
    virtual ~PhysicalWriter() = default;
    /**
     * Prepare the writer to ensure the length can fit into current block.
     *
     * @param length length of content
     * @return starting offset after preparing. If -1, means prepare has failed,
     * due to the specified length cannot fit into current block.
     */
    virtual std::int64_t prepare(int length) = 0;
    /**
     * Append content to the file.
     *
     * @param buffer content buffer container
     * @param offset start offset of actual content buffer
     * @param length length of actual content buffer
     * @return start offset of content in the file.
     */
    virtual std::int64_t append(const uint8_t *buffer, int offset, int length) = 0;
    /**
     * Append content to the file.
     * @param buffer content buffer
     * @return start offset of content in the file
     */
     virtual std::int64_t append(std::shared_ptr<ByteBuffer> byteBuffer) =0 ;
    /**
     * Close writer.
     */
    virtual void close() = 0;
    /**
     * Flush writer.
     */
    virtual void flush() = 0;

    virtual std::string getPath() const = 0;

    virtual int getBufferSize() const = 0;
};
#endif //PIXELS_PHYSICALWRITER_H
