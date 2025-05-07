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
 * @create 25-04-01
 */

#ifndef PIXELS_LONGCOLUMNWRITER_H
#define PIXELS_LONGCOLUMNWRITER_H

#include "encoding/RunLenIntEncoder.h"
#include "ColumnWriter.h"

class LongColumnWriter : public ColumnWriter
{
public:
  LongColumnWriter(std::shared_ptr <TypeDescription> type, std::shared_ptr <PixelsWriterOption> writerOption);

  int write(std::shared_ptr <ColumnVector> vector, int length) override;

  void close() override;

  void newPixel() override;

  bool decideNullsPadding(std::shared_ptr <PixelsWriterOption> writerOption) override;

  pixels::proto::ColumnEncoding getColumnChunkEncoding() const override;

private:
  bool runlengthEncoding;
  std::unique_ptr <RunLenIntEncoder> encoder;
  std::vector<long> curPixelVector; // current pixel value vector haven't written out yet

  void writeCurPartLong(std::shared_ptr <ColumnVector> columnVector, long *values, int curPartLength, int curPartOffset);
};

#endif // PIXELS_LONGCOLUMNWRITER_H
