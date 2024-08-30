//
// Created by liyu on 3/21/23.
//
#ifndef PIXELS_RUNLENINTENCODER_H
#define PIXELS_RUNLENINTENCODER_H
/**
 * @file RunLenIntEncoder.h
 * @author khaiwang
 * @date 2023-08-09
 */

// -----------------------------------------------------------
#include "Encoder.h"
#include "utils/EncodingUtils.h"
// -----------------------------------------------------------
#include <cstdint>
#include <memory>
// -----------------------------------------------------------

using byte = uint8_t;

class RunLenIntEncoder: public Encoder {
public:
    // PENDING: we need a type to denote that this encoder is not inited (INIT)
    enum EncodingType {
        SHORT_REPEAT, DIRECT, PATCHED_BASE, DELTA, INIT
    };
    // -----------------------------------------------------------
    // Construtors 
    RunLenIntEncoder();

    RunLenIntEncoder(bool isSigned, bool isAlignedBitPacking);
    ~RunLenIntEncoder();

    // -----------------------------------------------------------
    // Encoding functions
    void encode(long* values, int offset, int length, byte* results, int& resultLength);
    void encode(int* values, int offset, int length, byte* results, int& resultLength);
    void encode(long* values, byte* results, int length, int& resultLength);
    void encode(int* values, byte* results, int length, int& resultLength);
    // -----------------------------------------------------------
    void determineEncoding();
    // -----------------------------------------------------------
    void writeValues();
    void writeShortRepeatValues();
    void writeDirectValues();
    void writePatchedBaseValues();
    void writeDeltaValues();
    void writeInts(long* input, int offset, int len, int bitSize);
    int getOpcode();
    void write(long value);
    void flush();
    void initializeLiterals(long value);
    void clear();
    int percentileBits(long* data, int offset, int length, double p);
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
    void writeVslong(std::shared_ptr<ByteBuffer> output, long value);
    void writeVulong(std::shared_ptr<ByteBuffer> output, long value);


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
    long* gapVsPatchList;
    int gapVsPatchListSize;
    // -----------------------------------------------------------
    int zzBits90p;
    int zzBits100p;
    int brBits95p;
    int brBits100p;
    
    long min;

    long* literals;
    long* zigzagLiterals;
    long* baseRedLiterals;
    long* adjDeltas;

    EncodingUtils encodingUtils;
    // PENDING: should use byte buffer here? ref @Decoder
    std::shared_ptr<ByteBuffer> outputStream;

};
#endif //PIXELS_RUNLENINTENCODER_H
