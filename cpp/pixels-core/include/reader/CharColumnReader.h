//
// Created by liyu on 3/19/23.
//

#ifndef PIXELS_CHARCOLUMNREADER_H
#define PIXELS_CHARCOLUMNREADER_H

#include "reader/StringColumnReader.h"
#include "reader/ColumnReader.h"

class CharColumnReader: public StringColumnReader {
public:
    explicit CharColumnReader(std::shared_ptr<TypeDescription> type);
};
#endif //PIXELS_CHARCOLUMNREADER_H
