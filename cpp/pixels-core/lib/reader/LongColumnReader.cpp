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
//
// Created by whz on 4/1/25.
//

#include "reader/LongColumnReader.h"
#include "vector/LongColumnVector.h"

LongColumnReader::LongColumnReader(std::shared_ptr<TypeDescription> type)
    : ColumnReader(type) {
  // TODO: implement
}

void LongColumnReader::close() {
  // TODO: implement
}

void LongColumnReader::read(std::shared_ptr<ByteBuffer> input,
                            pixels::proto::ColumnEncoding &encoding, int offset,
                            int size, int pixelStride, int vectorIndex,
                            std::shared_ptr<ColumnVector> vector,
                            pixels::proto::ColumnChunkIndex &chunkIndex,
                            std::shared_ptr<PixelsBitMask> filterMask) {
  std::shared_ptr<LongColumnVector> columnVector =
      std::static_pointer_cast<LongColumnVector>(vector);

  // Make sure [offset, offset + size) is in the same pixels.
  assert(offset / pixelStride == (offset + size - 1) / pixelStride);

  // if read from start, init the stream and decoder
  if (offset == 0) {
    decoder = std::make_shared<RunLenIntDecoder>(input, true);
    ColumnReader::elementIndex = 0;
    isNullOffset = chunkIndex.isnulloffset();
  }

  int pixelId = elementIndex / pixelStride;
  bool hasNull = chunkIndex.pixelstatistics(pixelId).statistic().hasnull();
  setValid(input, pixelStride, vector, pixelId, hasNull);

  if (encoding.kind() == pixels::proto::ColumnEncoding_Kind_RUNLENGTH) {
    for (int i = 0; i < size; i++) {
      columnVector->longVector[i + vectorIndex] = decoder->next();

      elementIndex++;
    }
  } else {
    columnVector->longVector =
        (int64_t *)(input->getPointer() + input->getReadPos());
  }
}