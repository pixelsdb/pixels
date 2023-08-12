//
// Created by liyu on 3/14/23.
//

#ifndef PIXELS_PIXELSFILEMAGICINVALIDEXCEPTION_H
#define PIXELS_PIXELSFILEMAGICINVALIDEXCEPTION_H
#include <exception>
#include <string>
#include <iostream>
class PixelsFileMagicInvalidException: public std::exception {
public:
    PixelsFileMagicInvalidException(const std::string & magic);
};
#endif //PIXELS_PIXELSFILEMAGICINVALIDEXCEPTION_H
