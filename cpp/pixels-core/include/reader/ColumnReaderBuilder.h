//
// Created by liyu on 3/19/23.
//

#ifndef PIXELS_COLUMNREADERBUILDER_H
#define PIXELS_COLUMNREADERBUILDER_H

#include "reader/ColumnReader.h"
#include "reader/IntegerColumnReader.h"
#include "reader/CharColumnReader.h"
#include "reader/VarcharColumnReader.h"
#include "reader/DecimalColumnReader.h"
#include "reader/DateColumnReader.h"
#include "reader/TimestampColumnReader.h"

class ColumnReaderBuilder {
public:
    static std::shared_ptr<ColumnReader> newColumnReader(
            std::shared_ptr<TypeDescription> type);
};
#endif //PIXELS_COLUMNREADERBUILDER_H
