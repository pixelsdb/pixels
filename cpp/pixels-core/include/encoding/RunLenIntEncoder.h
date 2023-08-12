//
// Created by liyu on 3/21/23.
//

#ifndef PIXELS_RUNLENINTENCODER_H
#define PIXELS_RUNLENINTENCODER_H

#include "encoding/Encoder.h"

class RunLenIntEncoder: public Encoder {
public:
    enum EncodingType {
        SHORT_REPEAT, DIRECT, PATCHED_BASE, DELTA
    };

};
#endif //PIXELS_RUNLENINTENCODER_H
