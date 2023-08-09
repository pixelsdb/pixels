//
// Created by liyu on 7/6/23.
//

#include "PixelsBitMask.h"
#include <math.h>

PixelsBitMask::PixelsBitMask(long length) {
    this->maskLength = length;
    this->arrayLength = std::ceil(1.0 * length / 8);
    posix_memalign(reinterpret_cast<void **>(&mask), 4096, arrayLength);
    memset(mask, 255, arrayLength);
}

PixelsBitMask::PixelsBitMask(PixelsBitMask &other) {
    maskLength = other.maskLength;
    arrayLength = other.arrayLength;
    posix_memalign(reinterpret_cast<void **>(&mask), 4096, arrayLength);
    memcpy(mask, other.mask, arrayLength);
}

PixelsBitMask::~PixelsBitMask() {
    free(mask);
    mask = nullptr;
}

bool PixelsBitMask::isNone() {
    for(int i = 0; i < arrayLength - 1; i++) {
        if(mask[i] != 0) {
            return false;
        }
    }
    uint8_t lastByte = mask[arrayLength - 1];
    uint8_t lastMask = (uint16_t)(1 << (maskLength - 8 * (arrayLength - 1))) - 1;
    return !(lastByte & lastMask);
}

void PixelsBitMask::Or(PixelsBitMask &other) {
    // if their maskLength are the same, the arrayLength must be the same
    assert(other.maskLength == maskLength);
    for(int i = 0; i < arrayLength; i++) {
        mask[i] = mask[i] | other.mask[i];
    }
}

void PixelsBitMask::And(PixelsBitMask &other) {
// if their maskLength are the same, the arrayLength must be the same
    assert(other.maskLength == maskLength);
    for(int i = 0; i < arrayLength; i++) {
        mask[i] = mask[i] & other.mask[i];
    }
}

void PixelsBitMask::set() {
    memset(mask, 255, arrayLength);
}

void PixelsBitMask::set(long index, uint8_t value) {
    assert(index < maskLength);
    uint8_t & byteMask = mask[index / 8];
    uint8_t shiftMask = 1 << (index % 8);
    if(value == 0) {
        byteMask = byteMask & ~(shiftMask);
    } else {
        byteMask = byteMask | shiftMask;
    }
}



uint8_t PixelsBitMask::get(long index) {
    uint8_t & byteMask = mask[index / 8];
    uint8_t shiftMask = 1 << (index % 8);
    return bool(byteMask & shiftMask);
}

void PixelsBitMask::Or(long index, uint8_t value) {
    if(value == 1) {
        assert(index < maskLength);
        uint8_t & byteMask = mask[index / 8];
        uint8_t shiftMask = 1 << (index % 8);
        byteMask = byteMask | shiftMask;
    }
}

void PixelsBitMask::And(long index, uint8_t value) {
    if(value == 0) {
        assert(index < maskLength);
        uint8_t & byteMask = mask[index / 8];
        uint8_t shiftMask = 1 << (index % 8);
        byteMask = byteMask & ~(shiftMask);
    }
}

void PixelsBitMask::setByteAligned(long index, uint8_t value) {
    mask[index / 8] = value;
}


