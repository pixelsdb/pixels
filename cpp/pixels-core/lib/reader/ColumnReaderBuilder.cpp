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
#include "reader/ColumnReaderBuilder.h"
#include "exception/InvalidArgumentException.h"

std::shared_ptr <ColumnReader> ColumnReaderBuilder::newColumnReader(std::shared_ptr <TypeDescription> type)
{
    switch (type->getCategory())
    {
//        case TypeDescription::BOOLEAN:
//            break;
//        case TypeDescription::BYTE:
//            break;
        case TypeDescription::SHORT:
        case TypeDescription::INT:
        case TypeDescription::LONG:
            return std::make_shared<IntegerColumnReader>(type);
//        case TypeDescription::FLOAT:
//            break;
//        case TypeDescription::DOUBLE:
//            break;
        case TypeDescription::DECIMAL:
        {
            if (type->getPrecision() <= TypeDescription::SHORT_DECIMAL_MAX_PRECISION)
            {
                return std::make_shared<DecimalColumnReader>(type);
            }
            else
            {
                throw InvalidArgumentException("Currently we didn't implement LongDecimalColumnVector.");
            }
        }
//        case TypeDescription::STRING:
//            break;
        case TypeDescription::DATE:
            return std::make_shared<DateColumnReader>(type);
//        case TypeDescription::TIME:
//            break;
        case TypeDescription::TIMESTAMP:
            return std::make_shared<TimestampColumnReader>(type);
//        case TypeDescription::VARBINARY:
//            break;
//        case TypeDescription::BINARY:
//            break;
        case TypeDescription::VARCHAR:
            return std::make_shared<VarcharColumnReader>(type);
        case TypeDescription::CHAR:
            return std::make_shared<CharColumnReader>(type);
//        case TypeDescription::STRUCT:
//            break;
        default:
            throw InvalidArgumentException(
                    "bad column type in ColumnReaderBuilder: " + std::to_string(type->getCategory()));
    }
}
