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
 * @author whz
 * @create 2024-11-29
 */
#ifndef PIXELS_COLUMNWRITERBUILDER_H
#define PIXELS_COLUMNWRITERBUILDER_H

#include "writer/ColumnWriter.h"
#include "writer/PixelsWriterOption.h"
#include "writer/ColumnWriterBuilder.h"
#include "writer/IntColumnWriter.h"
#include "writer/LongColumnWriter.h"
#include "writer/DateColumnWriter.h"
#include "writer/TimestampColumnWriter.h"
#include "writer/DecimalColumnWriter.h"
#include "writer/StringColumnWriter.h"

class ColumnWriterBuilder
{
public:
    static std::shared_ptr <ColumnWriter>
    newColumnWriter(std::shared_ptr <TypeDescription> type, std::shared_ptr <PixelsWriterOption> writerOption);
};
#endif // PIXELS_COLUMNWRITERBUILDER_H


