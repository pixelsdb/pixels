//
// Created by liyu on 3/13/23.
//

#ifndef PIXELS_PIXELSRUNTIMEEXCEPTION_H
#define PIXELS_PIXELSRUNTIMEEXCEPTION_H

#include "PixelsVersion.h"
#include <cstdint>
#include <exception>
#include <iostream>
#include <string>

class PixelsFileVersionInvalidException : public std::exception {
  public:
    explicit PixelsFileVersionInvalidException(uint32_t version);
};
#endif // PIXELS_PIXELSRUNTIMEEXCEPTION_H
