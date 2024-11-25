//
// Created by gengdy on 24-11-25.
//

#include "writer/PixelsWriterOption.h"

PixelsWriterOption::PixelsWriterOption() {}

int PixelsWriterOption::getPixelsStride() const {
    return this->pixelsStride;
}

std::shared_ptr<PixelsWriterOption> PixelsWriterOption::setPixelsStride(int pixelsStride) {
    this->pixelsStride = pixelsStride;
    return std::shared_ptr<PixelsWriterOption>(this);
}

EncodingLevel PixelsWriterOption::getEncodingLevel() {
    return this->encodingLevel;
}

std::shared_ptr<PixelsWriterOption> PixelsWriterOption::setEncodingLevel(EncodingLevel encodingLevel) {
    this->encodingLevel = encodingLevel;
    return std::shared_ptr<PixelsWriterOption>(this);
}

bool PixelsWriterOption::isNullsPadding() {
    return this->nullsPadding;
}

std::shared_ptr<PixelsWriterOption> PixelsWriterOption::setNullsPadding(bool nullsPadding) {
    this->nullsPadding = nullsPadding;
    return std::shared_ptr<PixelsWriterOption>(this);
}