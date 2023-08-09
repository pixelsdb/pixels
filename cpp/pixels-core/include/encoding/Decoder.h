//
// Created by liyu on 3/20/23.
//

#ifndef PIXELS_DECODER_H
#define PIXELS_DECODER_H

#include <memory>
#include <iostream>
#include "physical/natives/ByteBuffer.h"

class Decoder {
public:
    virtual void close() = 0;
    virtual long next() = 0;
	virtual bool hasNext() = 0;
};
#endif //PIXELS_DECODER_H
