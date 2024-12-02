//
// Created by liyu on 3/13/23.
//

#include "exception/PixelsFileVersionInvalidException.h"

PixelsFileVersionInvalidException::PixelsFileVersionInvalidException(uint32_t version) {
    std::cout<<"This is not a valid file version "
        << version
        <<" for current reader "
        << PixelsVersion::currentVersion()
        <<std::endl;
}
