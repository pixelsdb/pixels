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
        INIT, SHORT_REPEAT, DIRECT, PATCHED_BASE, DELTA
    };
    // -----------------------------------------------------------
    // Construtors 
    RunLenIntEncoder();

    RunLenIntEncoder(bool isSigned, bool isAlignedBitPacking);

    // -----------------------------------------------------------
    // Encoding functions
    void encode(const std::vector<long>& values, int offset, int length, std::vector<byte>& results);
    void encode(const std::vector<int>&, int offset, int length, std::vector<byte>& results);
    void encode(const std::vector<long>& values, std::vector<byte>& results);
    void encode(const std::vector<int>& values, std::vector<byte>& results);
    // -----------------------------------------------------------
    void determineEncoding();
    // -----------------------------------------------------------
    void writeValues();
    void writeShortRepeatValues();
    void writeDirectValues();
    void writePatchedBaseValues();
    void writeDeltaValues();
    void writeInts(const std::vector<long>& input, int offset, int len, int bitSize);
    int getOpcode();
    void write(long value);
    void initializeLiterals(long value);
    void clear();
    int percentileBits(const std::vector<long>& data, int offset, int length, double p);
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
    std::vector<long> gapVsPatchList;
    // -----------------------------------------------------------
    int zzBits90p;
    int zzBits100p;
    int brBits95p;
    int brBits100p;
    
    long min;

    std::vector<long> literals;
    std::vector<long> zigzagLiterals;
    std::vector<long> baseRedLiterals;
    std::vector<long> adjDeltas;

    EncodingUtils encodingUtils;
    // PENDING: should use byte buffer here? ref @Decoder
    std::shared_ptr<ByteBuffer> outputStream;

};
#endif //PIXELS_RUNLENINTENCODER_H
