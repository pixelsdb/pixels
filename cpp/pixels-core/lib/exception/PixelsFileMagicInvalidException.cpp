//
// Created by liyu on 3/14/23.
//

#include "exception/PixelsFileMagicInvalidException.h"

PixelsFileMagicInvalidException::PixelsFileMagicInvalidException(const std::string &magic) {
    std::cout<<"The file magic "
        << magic
        <<" is not valid."
        <<std::endl;
}
