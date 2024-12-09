//
// Created by whz on 24-11-29.
//
//#include "writer/ColumnWriterBuilder.h"
#include "writer/ColumnWriterBuilder.h"
#include "writer/IntegerColumnWriter.h"
#include "writer/testColumnWriter.h"

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
            break;
        case TypeDescription::TIME:
            break;
        case TypeDescription::VARBINARY:
            break;
        case TypeDescription::BINARY:
            break;
        case TypeDescription::STRUCT:
            break;
        default:
            throw InvalidArgumentException("bad column type in ColumnReaderBuilder: " + std::to_string(type->getCategory()));
    }
    return std::shared_ptr<ColumnWriter>();
}


