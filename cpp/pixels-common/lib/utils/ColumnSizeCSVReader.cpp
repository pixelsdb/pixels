//
// Created by liyu on 1/24/24.
//

#include "utils/ColumnSizeCSVReader.h"

int ColumnSizeCSVReader::get(const std::string & columnName) {
    if (!colSize.count(columnName)) {
        throw InvalidArgumentException("ColumnSizeCSVReader::get: wrong column name!");
    } else {
        return colSize[columnName];
    }
}