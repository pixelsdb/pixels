//
// Created by whz on 24-11-29.
//



#ifndef PIXELS_COLUMNWRITERBUILDER_H
#define PIXELS_COLUMNWRITERBUILDER_H
#include "writer/ColumnWriter.h"
#include "writer/PixelsWriterOption.h"

class ColumnWriterBuilder {
public:
    static std::shared_ptr<ColumnWriter>
    newColumnWriter(std::shared_ptr<TypeDescription> type, std::shared_ptr<PixelsWriterOption> writerOption);
};
#endif // PIXELS_COLUMNWRITERBUILDER_H


