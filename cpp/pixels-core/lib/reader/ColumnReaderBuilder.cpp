//
// Created by liyu on 3/19/23.
//

#include "reader/ColumnReaderBuilder.h"
#include "exception/InvalidArgumentException.h"

std::shared_ptr<ColumnReader> ColumnReaderBuilder::newColumnReader(std::shared_ptr<TypeDescription> type) {
    switch (type->getCategory()) {
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
	    case TypeDescription::DECIMAL: {
		    if (type->getPrecision() <= TypeDescription::SHORT_DECIMAL_MAX_PRECISION) {
			    return std::make_shared<DecimalColumnReader>(type);
		    } else {
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
            throw InvalidArgumentException("bad column type in ColumnReaderBuilder: " + std::to_string(type->getCategory()));
    }
}
