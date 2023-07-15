//
// Created by liyu on 7/6/23.
//

#include "PixelsFilterMask.h"
#include <math.h>

pixelsFilterMask::pixelsFilterMask(long length) {
    this->maskLength = length;
    this->arrayLength = std::ceil(1.0 * length / 8);
    posix_memalign(reinterpret_cast<void **>(&mask), 4096, arrayLength);
    memset(mask, 255, arrayLength);
}

pixelsFilterMask::pixelsFilterMask(pixelsFilterMask &other) {
    maskLength = other.maskLength;
    arrayLength = other.arrayLength;
    posix_memalign(reinterpret_cast<void **>(&mask), 4096, arrayLength);
    memcpy(mask, other.mask, arrayLength);
}

pixelsFilterMask::~pixelsFilterMask() {
    free(mask);
    mask = nullptr;
}

bool pixelsFilterMask::isNone() {
    for(int i = 0; i < arrayLength - 1; i++) {
        if(mask[i] != 0) {
            return false;
        }
    }
    uint8_t lastByte = mask[arrayLength - 1];
    uint8_t lastMask = (uint16_t)(1 << (maskLength - 8 * (arrayLength - 1))) - 1;
    return !(lastByte & lastMask);
}

void pixelsFilterMask::Or(pixelsFilterMask &other) {
    // if their maskLength are the same, the arrayLength must be the same
    assert(other.maskLength == maskLength);
    for(int i = 0; i < arrayLength; i++) {
        mask[i] = mask[i] | other.mask[i];
    }
}

void pixelsFilterMask::And(pixelsFilterMask &other) {
// if their maskLength are the same, the arrayLength must be the same
    assert(other.maskLength == maskLength);
    for(int i = 0; i < arrayLength; i++) {
        mask[i] = mask[i] & other.mask[i];
    }
}

void pixelsFilterMask::set() {
    memset(mask, 255, arrayLength);
}

void pixelsFilterMask::set(long index, uint8_t value) {
    assert(index < maskLength);
    uint8_t & byteMask = mask[index / 8];
    uint8_t shiftMask = 1 << (index % 8);
    if(value == 0) {
        byteMask = byteMask & ~(shiftMask);
    } else {
        byteMask = byteMask | shiftMask;
    }
}



uint8_t pixelsFilterMask::get(long index) {
    uint8_t & byteMask = mask[index / 8];
    uint8_t shiftMask = 1 << (index % 8);
    return bool(byteMask & shiftMask);
}

void pixelsFilterMask::Or(long index, uint8_t value) {
    if(value == 1) {
        assert(index < maskLength);
        uint8_t & byteMask = mask[index / 8];
        uint8_t shiftMask = 1 << (index % 8);
        byteMask = byteMask | shiftMask;
    }
}

void pixelsFilterMask::And(long index, uint8_t value) {
    if(value == 0) {
        assert(index < maskLength);
        uint8_t & byteMask = mask[index / 8];
        uint8_t shiftMask = 1 << (index % 8);
        byteMask = byteMask & ~(shiftMask);
    }
}

void pixelsFilterMask::setByteAligned(long index, uint8_t value) {
    mask[index / 8] = value;
}


