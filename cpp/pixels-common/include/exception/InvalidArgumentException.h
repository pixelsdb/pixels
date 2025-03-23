//
// Created by liyu on 3/13/23.
//

#ifndef PIXELS_INVALIDARGUMENTEXCEPTION_H
#define PIXELS_INVALIDARGUMENTEXCEPTION_H

#include<iostream>
#include<exception>

class InvalidArgumentException: public std::exception {
public:
    InvalidArgumentException() = default;
    explicit InvalidArgumentException(std::string message);
};
#endif //PIXELS_INVALIDARGUMENTEXCEPTION_H
