//
// Created by gengdy on 24-11-25.
//

#ifndef PIXELS_PIXELSWRITEROPTION_H
#define PIXELS_PIXELSWRITEROPTION_H

#include "encoding/EncodingLevel.h"
#include <memory>

class PixelsWriterOption {
public:
    PixelsWriterOption();
    int getPixelsStride() const;
    std::shared_ptr<PixelsWriterOption> setPixelsStride(int pixelsStride);
    EncodingLevel getEncodingLevel();
    std::shared_ptr<PixelsWriterOption> setEncodingLevel(EncodingLevel encodingLevel);
    bool isNullsPadding();
    std::shared_ptr<PixelsWriterOption> setNullsPadding(bool nullsPadding);
private:
    int pixelsStride;
    EncodingLevel encodingLevel;
    /**
     * Whether nulls positions in column are padded by arbitrary values and occupy storage and memory space.
     */
    bool nullsPadding;
};
#endif //PIXELS_PIXELSWRITEROPTION_H
