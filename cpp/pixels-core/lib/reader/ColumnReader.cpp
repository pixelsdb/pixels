//
// Created by liyu on 3/19/23.
//

#include "reader/ColumnReader.h"

ColumnReader::ColumnReader(std::shared_ptr<TypeDescription> type) {
    this->type = type;
    this->elementIndex = 0;
    this->hasNull = true;
}

std::shared_ptr<ColumnReader> ColumnReader::newColumnReader(std::shared_ptr<TypeDescription> type) {
    switch(type->getCategory()) {
        case TypeDescription::BOOLEAN:
            break;
        case TypeDescription::BYTE:
            break;
        case TypeDescription::SHORT:
            break;
        case TypeDescription::INT:
            break;
        case TypeDescription::LONG:
            break;
        case TypeDescription::FLOAT:
            break;
        case TypeDescription::DOUBLE:
            break;
        case TypeDescription::DECIMAL:
            break;
        case TypeDescription::STRING:
            break;
        case TypeDescription::DATE:
            break;
        case TypeDescription::TIME:
            break;
        case TypeDescription::TIMESTAMP:
            break;
        case TypeDescription::VARBINARY:
            break;
        case TypeDescription::BINARY:
            break;
        case TypeDescription::VARCHAR:
            break;
        case TypeDescription::CHAR:
            break;
        case TypeDescription::STRUCT:
            break;
    }
	throw InvalidArgumentException("This function is not supported yet. ");
}
