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
 * @create 2023-03-19
 */
#ifndef PIXELS_COLUMNREADERBUILDER_H
#define PIXELS_COLUMNREADERBUILDER_H

#include "reader/ColumnReader.h"
#include "reader/CharColumnReader.h"
#include "reader/VarcharColumnReader.h"
#include "reader/DecimalColumnReader.h"
#include "reader/DateColumnReader.h"
#include "reader/TimestampColumnReader.h"
#include "reader/IntColumnReader.h"
#include "reader/LongColumnReader.h"
#include "reader/StringColumnReader.h"

class ColumnReaderBuilder
{
public:
    static std::shared_ptr <ColumnReader> newColumnReader(
            std::shared_ptr <TypeDescription> type);
};
#endif //PIXELS_COLUMNREADERBUILDER_H
