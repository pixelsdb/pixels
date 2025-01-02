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
 * @create 2023-04-05
 */
#include "reader/DecimalColumnReader.h"

/**
 * The column reader of decimals.
 * <p><b>Note: it only supports short decimals with max precision and scale 18.</b></p>
 * @author hank
 */
DecimalColumnReader::DecimalColumnReader(std::shared_ptr <TypeDescription> type) : ColumnReader(type)
{

}

void DecimalColumnReader::close()
{

}

void DecimalColumnReader::read(std::shared_ptr <ByteBuffer> input, pixels::proto::ColumnEncoding &encoding, int offset,
                               int size, int pixelStride, int vectorIndex, std::shared_ptr <ColumnVector> vector,
                               pixels::proto::ColumnChunkIndex &chunkIndex, std::shared_ptr <PixelsBitMask> filterMask)
{
    std::shared_ptr <DecimalColumnVector> columnVector =
            std::static_pointer_cast<DecimalColumnVector>(vector);
    if (type->getPrecision() != columnVector->getPrecision() || type->getScale() != columnVector->getScale())
    {
        throw InvalidArgumentException("reader of decimal(" + std::to_string(type->getPrecision())
                                       + "," + std::to_string(type->getScale()) + ") doesn't match the column "
                                                                                  "vector of decimal(" +
                                       std::to_string(columnVector->getPrecision()) + ","
                                       + std::to_string(columnVector->getScale()) + ")");
    }
    if (offset == 0)
    {
        // TODO: here we check null
        ColumnReader::elementIndex = 0;
        isNullOffset = chunkIndex.isnulloffset();
    }
    // TODO: we didn't implement the run length encoded method

    int pixelId = elementIndex / pixelStride;
    bool hasNull = chunkIndex.pixelstatistics(pixelId).statistic().hasnull();
    setValid(input, pixelStride, vector, pixelId, hasNull);

    columnVector->vector = (long *) (input->getPointer() + input->getReadPos());
    input->setReadPos(input->getReadPos() + size * sizeof(long));


}
