//
// Created by liyu on 3/14/23.
//

#ifndef PIXELS_PIXELSREADEREXCEPTION_H
#define PIXELS_PIXELSREADEREXCEPTION_H
#include <exception>
#include <string>
#include <iostream>
class PixelsReaderException: public std::exception {
public:
    explicit PixelsReaderException(const std::string & msg);
};
#endif //PIXELS_PIXELSREADEREXCEPTION_H
