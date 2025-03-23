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
// Created by whz on 24-11-29.
//
//#include "writer/ColumnWriterBuilder.h"
#include "writer/ColumnWriterBuilder.h"
#include "writer/IntegerColumnWriter.h"
#include "writer/DateColumnWriter.h"
#include "writer/TimestampColumnWriter.h"
#include "writer/DecimalColumnWriter.h"
#include "writer/StringColumnWriter.h"

std::shared_ptr<ColumnWriter> ColumnWriterBuilder::newColumnWriter(std::shared_ptr<TypeDescription> type, std::shared_ptr<PixelsWriterOption> writerOption) {
    switch(type->getCategory()) {
        case TypeDescription::SHORT:
        case TypeDescription::INT:
        case TypeDescription::LONG:
//            return std::dynamic_pointer_cast<ColumnWriter,IntegerColumnWriter>(std::make_shared<IntegerColumnWriter>(type, writerOption));
            return std::make_shared<IntegerColumnWriter>(type, writerOption);
        case TypeDescription::BOOLEAN:
            break;
        case TypeDescription::BYTE:
            break;
        case TypeDescription::FLOAT:
            break;
        case TypeDescription::DOUBLE:
            break;
        case TypeDescription::STRING:
            return std::make_shared<StringColumnWriter>(type, writerOption);
        case TypeDescription::TIME:
            break;
        case TypeDescription::VARBINARY:
            break;
        case TypeDescription::BINARY:
            break;
        case TypeDescription::STRUCT:
            break;
        case TypeDescription::DATE:
            return std::make_shared<DateColumnWriter>(type, writerOption);
        case TypeDescription::TIMESTAMP:
            return std::make_shared<TimestampColumnWriter>(type, writerOption);
        case TypeDescription::DECIMAL:
            return std::make_shared<DecimalColumnWriter>(type, writerOption);
        default:
            throw InvalidArgumentException("bad column type in ColumnWriterBuilder: " + std::to_string(type->getCategory()));
    }
    return std::shared_ptr<ColumnWriter>();
}


