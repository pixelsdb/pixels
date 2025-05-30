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

/*
 * @author whz
 * @create 2025-04-01
 */

#ifndef PIXELS_INTCOLUMNREADER_H
#define PIXELS_INTCOLUMNREADER_H

#include "reader/ColumnReader.h"
#include "encoding/RunLenIntDecoder.h"

class IntColumnReader : public ColumnReader
{
 public:
  explicit IntColumnReader(std::shared_ptr<TypeDescription> type);

  void close() override;

  void read(std::shared_ptr<ByteBuffer> input,
            pixels::proto::ColumnEncoding &encoding,
            int offset, int size, int pixelStride,
            int vectorIndex, std::shared_ptr<ColumnVector> vector,
            pixels::proto::ColumnChunkIndex &chunkIndex,
            std::shared_ptr<PixelsBitMask> filterMask) override;

 private:
  std::shared_ptr<RunLenIntDecoder> decoder;
};

#endif // PIXELS_INTCOLUMNREADER_H
