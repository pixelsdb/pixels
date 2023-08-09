//
// Created by liyu on 3/19/23.
//

#ifndef PIXELS_VARCHARCOLUMNREADER_H
#define PIXELS_VARCHARCOLUMNREADER_H

#include "reader/StringColumnReader.h"
#include "reader/ColumnReader.h"
class VarcharColumnReader: public StringColumnReader {
public:
    explicit VarcharColumnReader(std::shared_ptr<TypeDescription> type);

};
#endif //PIXELS_VARCHARCOLUMNREADER_H
