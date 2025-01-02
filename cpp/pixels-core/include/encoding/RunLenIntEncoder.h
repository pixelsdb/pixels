/*
 * Copyright 2023 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */

/*
 * @author khaiwang
 * @create 2023-08-09
 */
#ifndef PIXELS_RUNLENINTENCODER_H
#define PIXELS_RUNLENINTENCODER_H

// -----------------------------------------------------------
#include "Encoder.h"
#include "utils/EncodingUtils.h"
// -----------------------------------------------------------
#include <cstdint>
#include <memory>
// -----------------------------------------------------------

using byte = uint8_t;

class RunLenIntEncoder : public Encoder
{
public:
    // PENDING: we need a type to denote that this encoder is not inited (INIT)
    enum EncodingType
    {
        SHORT_REPEAT, DIRECT, PATCHED_BASE, DELTA, INIT
    };

    // -----------------------------------------------------------
    // Construtors 
    RunLenIntEncoder();

    RunLenIntEncoder(bool isSigned, bool isAlignedBitPacking);

    ~RunLenIntEncoder();

    // -----------------------------------------------------------
    // Encoding functions
    void encode(long *values, int offset, int length, byte *results, int &resultLength);

    void encode(int *values, int offset, int length, byte *results, int &resultLength);

    void encode(long *values, byte *results, int length, int &resultLength);

    void encode(int *values, byte *results, int length, int &resultLength);

    // -----------------------------------------------------------
    void determineEncoding();

    // -----------------------------------------------------------
    void writeValues();

    void writeShortRepeatValues();

    void writeDirectValues();

    void writePatchedBaseValues();

    void writeDeltaValues();

    void writeInts(long *input, int offset, int len, int bitSize);

    int getOpcode();

    void write(long value);

    void flush();

    void initializeLiterals(long value);

    void clear();

    int percentileBits(long *data, int offset, int length, double p);

    int findClosestNumBits(long value);

    bool isSafeSubtract(long left, long right);

    int getClosestAlignedFixedBits(int n);

    // -----------------------------------------------------------
    // PATCH_BASE
    void preparePatchedBlob();

    // -----------------------------------------------------------
    // zigzag 
    void computeZigZagLiterals();

    long zigzagEncode(long val);

    void writeVslong(std::shared_ptr <ByteBuffer> output, long value);

    void writeVulong(std::shared_ptr <ByteBuffer> output, long value);


// -----------------------------------------------------------
private:
    EncodingType encodingType;
    int numLiterals;
    int fixedRunLength;
    int variableRunLength;
    bool isSigned;
    bool isAlignedBitPacking;
    // -----------------------------------------------------------
    long prevDelta;
    long fixedDelta;
    int bitsDeltaMax;
    bool isFixedDelta;
    // -----------------------------------------------------------
    int patchWidth;
    int patchGapWidth;
    int patchLength;
    long *gapVsPatchList;
    int gapVsPatchListSize;
    // -----------------------------------------------------------
    int zzBits90p;
    int zzBits100p;
    int brBits95p;
    int brBits100p;

    long min;

    long *literals;
    long *zigzagLiterals;
    long *baseRedLiterals;
    long *adjDeltas;

    EncodingUtils encodingUtils;
    // PENDING: should use byte buffer here? ref @Decoder
    std::shared_ptr <ByteBuffer> outputStream;

};
#endif //PIXELS_RUNLENINTENCODER_H
