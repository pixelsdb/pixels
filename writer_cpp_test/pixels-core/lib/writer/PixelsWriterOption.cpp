//
// Created by gengdy on 24-11-25.
//

#include "writer/PixelsWriterOption.h"
#include <iostream>

PixelsWriterOption::PixelsWriterOption() {}

int PixelsWriterOption::getPixelsStride() const {
    return this->pixelsStride;
}

std::shared_ptr<PixelsWriterOption> PixelsWriterOption::setPixelsStride(int pixelsStride) {
    this->pixelsStride = pixelsStride;
    return shared_from_this();
}

EncodingLevel PixelsWriterOption::getEncodingLevel() const {
    return this->encodingLevel;
}

std::shared_ptr<PixelsWriterOption> PixelsWriterOption::setEncodingLevel(EncodingLevel encodingLevel) {
    this->encodingLevel = encodingLevel;
    return shared_from_this();
}

bool PixelsWriterOption::isNullsPadding() const {
    return this->nullsPadding;
}

std::shared_ptr<PixelsWriterOption> PixelsWriterOption::setNullsPadding(bool nullsPadding) {
    this->nullsPadding = nullsPadding;
    return shared_from_this();
}

ByteOrder PixelsWriterOption::getByteOrder() const {
    return byteOrder;
}

