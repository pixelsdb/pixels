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
// Created by whz on 12/9/24.
//

#ifndef DUCKDB_TIMESTAMPCOLUMNWRITER_H
#define DUCKDB_TIMESTAMPCOLUMNWRITER_H
#include "ColumnWriter.h"
#include "encoding/RunLenIntEncoder.h"

class TimestampColumnWriter : public ColumnWriter{
public:
    TimestampColumnWriter(std::shared_ptr<TypeDescription> type, std::shared_ptr<PixelsWriterOption> writerOption);

    int write(std::shared_ptr<ColumnVector> vector, int length) override;
    bool decideNullsPadding(std::shared_ptr<PixelsWriterOption> writerOption) override;
private:
    bool runlengthEncoding;
    std::unique_ptr<RunLenIntEncoder> encoder;
    std::vector<long> curPixelVector; // current pixel value vector haven't written out yet

};
#endif //DUCKDB_TIMESTAMPCOLUMNWRITER_H
