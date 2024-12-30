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
#include "encoding/RunLenIntEncoder.h"
#include "utils/Constants.h"


#include <memory>

// -----------------------------------------------------------
// Construtors 

RunLenIntEncoder::RunLenIntEncoder() : RunLenIntEncoder(true, true)
{}


/**
 * @brief Construct a new Run Length Integer Encoder with flags
 * 
 * @param isSigned 
 * @param isAlignedBitPacking 
 */
RunLenIntEncoder::RunLenIntEncoder(bool isSigned, bool isAlignedBitPacking) :
        isSigned(isSigned),
        isAlignedBitPacking(isAlignedBitPacking)
{
    // PENDING: will the byte buffer be used in a buffer pool
    //          so that we do not need to create it here
    outputStream = std::make_shared<ByteBuffer>();
    literals = new long[Constants::MAX_SCOPE];
    zigzagLiterals = new long[Constants::MAX_SCOPE];
    baseRedLiterals = new long[Constants::MAX_SCOPE];
    adjDeltas = new long[Constants::MAX_SCOPE];
    gapVsPatchList = new long[Constants::MAX_SCOPE];
    clear();
}

RunLenIntEncoder::~RunLenIntEncoder()
{
    if (literals != nullptr)
    {
        delete[] literals;
        literals = nullptr;
    }
    if (zigzagLiterals != nullptr)
    {
        delete[] zigzagLiterals;
        zigzagLiterals = nullptr;
    }
    if (baseRedLiterals != nullptr)
    {
        delete[] baseRedLiterals;
        baseRedLiterals = nullptr;
    }
    if (adjDeltas != nullptr)
    {
        delete[] adjDeltas;
        adjDeltas = nullptr;
    }
    if (gapVsPatchList != nullptr)
    {
        delete[] gapVsPatchList;
        gapVsPatchList = nullptr;
    }
}


// -----------------------------------------------------------
// Encoding Handles
void RunLenIntEncoder::encode(long *values, int offset, int length, byte *results, int &resLen)
{
    for (int i = 0; i < length; ++i)
    {
        // std::cout << encodingType << " value : " << values[i + offset] << std::endl;
        this->write(values[i + offset]);
    }
    flush();
    // std::cout << "length: " << length << std::endl;
    // std::cout << "buffer end: " << outputStream->getWritePos() << std::endl;
    resLen = outputStream->getWritePos();
    outputStream->getBytes(results, resLen);
    outputStream->resetPosition();
}

void RunLenIntEncoder::encode(int *values, int offset, int length, byte *results, int &resLen)
{
    for (int i = 0; i < length; ++i)
    {
        this->write(values[i + offset]);
    }
    flush();
    resLen = outputStream->getWritePos();
    outputStream->getBytes(results, resLen);
    outputStream->clear();
}

void RunLenIntEncoder::encode(long *values, byte *results, int length, int &resLen)
{
    encode(values, 0, length, results, resLen);
}

void RunLenIntEncoder::encode(int *values, byte *results, int length, int &resLen)
{
    encode(values, 0, length, results, resLen);
}


// -----------------------------------------------------------

void RunLenIntEncoder::determineEncoding()
{
    // std::cout << "determineEncoding()" << std::endl;
    // compute zigzag values for direct encoding if break early 
    // for delta overflows or shoter runs
    computeZigZagLiterals();
    zzBits100p = percentileBits(zigzagLiterals, 0, numLiterals, 1.0);

    // less than min repeat num so direct encoding
    if (numLiterals <= Constants::MIN_REPEAT)
    {
        // std::cout << "numLiterals <= Constants::MIN_REPEAT" << std::endl;
        encodingType = EncodingType::DIRECT;
        return;
    }

    // -----------------------------------------------------------
    // DELTA encoding check
    // -----------------------------------------------------------
    // monotonic
    bool isIncreasing = true;
    bool isDecreasing = true;
    isFixedDelta = true;

    // delta
    min = literals[0];
    long max = literals[0];
    long initialDelta = literals[1] - literals[0];
    long currDelta = 0;
    long deltaMax = 0;
    adjDeltas[0] = initialDelta;

    // traverse the literals and probe the monocity and delta
    for (int i = 1; i < numLiterals; ++i)
    {
        long l1 = literals[i];
        long l0 = literals[i - 1];
        // monotonicity
        isIncreasing = (isIncreasing && (l0 <= l1));
        isDecreasing = (isDecreasing && (l0 >= l1));

        // delta
        currDelta = l1 - l0;
        min = std::min(min, l1);
        max = std::max(max, l1);
        isFixedDelta = (isFixedDelta && (currDelta == initialDelta));
        if (i > 1)
        {
            adjDeltas[i - 1] = std::abs(currDelta);
            deltaMax = std::max(deltaMax, adjDeltas[i - 1]);
        }
    }

    // if delta overflow happens, we use DIRECT encoding 
    // without checking PATCHED_BASE because direct is faster
    if (!isSafeSubtract(max, min))
    {
        // std::cout << "!isSafeSubtract(max, min)" << std::endl;
        encodingType = EncodingType::DIRECT;
        return;
    }

    // after checking safe subtraction, subtraction will never overflow

    // if min equals to max, but according to @write(),
    // it is a fixed run longer than 10, cannot be encoded with SHORT_REPEAT
    // and we use DELTA with delta = 0
    if (min == max)
    {
        assert(isFixedDelta);
        assert(currDelta == 0);
        fixedDelta = 0;
        // std::cout << "min == max" << std::endl;
        encodingType = EncodingType::DELTA;
        return;
    }

    // delta != 0, but is a fixed value
    if (isFixedDelta)
    {
        assert(currDelta == initialDelta);
        encodingType = EncodingType::DELTA;
        // std::cout << "isFixedDelta" << std::endl;
        fixedDelta = currDelta;
        return;
    }

    // if initialDelta = zero, we cannot identify increasing or decreasing
    // so we cannot use DELTA encoding
    if (initialDelta != 0)
    {
        // stores the number of bits required 
        // for packing delta blob in DELTA encoding
        bitsDeltaMax = findClosestNumBits(deltaMax);

        // monotonic condition
        if (isIncreasing || isDecreasing)
        {
            // std::cout << "isIncreasing || isDecreasing" << std::endl;
            encodingType = EncodingType::DELTA;
            return;
        }
    }

    // -----------------------------------------------------------
    // PATCHED_BASE encoding check
    // -----------------------------------------------------------

    // percentile values are computed for the zigzag encoded values. if the
    // number of bits requirement between 90th and 100th percentile varies
    // beyond a threshold then we need to patch the values. if the variation
    // is not significant then we can use direct encoding
    zzBits90p = percentileBits(zigzagLiterals, 0, numLiterals, 0.9);
    int diffBitsLH = zzBits100p - zzBits90p;

    // if the difference is larger than 1 we should patch the values
    if (diffBitsLH > 1)
    {
        for (int i = 0; i < numLiterals; ++i)
        {
            baseRedLiterals[i] = literals[i] - min;
        }

        // 95th percentile width is used to determine the max allowed value
        // after which patching will be done
        brBits95p = percentileBits(baseRedLiterals, 0, numLiterals, 0.95);
        // 100th percentile is used to compute the max patch width
        brBits100p = percentileBits(baseRedLiterals, 0, numLiterals, 1.0);

        // after base reducing the values, if the difference in bits between
        // 95th percentile and 100th percentile value is zero then there
        // is no point in patching the values, in which case we will
        // fallback to DIRECT encoding.
        // The decision to use patched base was based on zigzag values, but the
        // actual patching is done on base reduced literals.
        if (brBits100p - brBits95p != 0)
        {
            // std::cout << "brBits100p - brBits95p != 0" << std::endl;
            encodingType = EncodingType::PATCHED_BASE;
            preparePatchedBlob();
            return;
        }
        else
        {
            // std::cout << "brBits100p - brBits95p == 0" << std::endl;
            encodingType = EncodingType::DIRECT;
            return;
        }
    }
    else
    {
        // if difference in bits between 90th and 100th percentile
        // is 0, patch length is 0, fallback to DIRECT
        // std::cout << "diffBitsLH <= 1" << std::endl;
        encodingType = EncodingType::DIRECT;
        return;
    }
}

void RunLenIntEncoder::preparePatchedBlob()
{
    // mask will be the max value beyond which patch will be generated
    long mask = (1l << brBits95p) - 1;

    // the size of gap and patch array only contains 5% values
    patchLength = (int) std::ceil(numLiterals * 0.05);

    auto gapList = std::vector<int>();
    gapList.resize(patchLength);
    auto patchList = std::vector<long>();
    patchList.resize(patchLength);

    // number of bits for patch
    patchWidth = encodingUtils.getClosestFixedBits(brBits100p - brBits95p);

    // if patch bit requirement is 64, it will be impossible to pack
    // the gap and the patch together in a long type
    // so we need to adjust the patch width
    if (patchWidth == 64)
    {
        patchWidth = 56;
        brBits95p = 8;
        mask = (1l << brBits95p) - 1;
    }

    int gapIdx = 0;
    int patchIdx = 0;
    int prev = 0;
    int gap = 0;
    int maxGap = 0;

    for (int i = 0; i < numLiterals; ++i)
    {
        // is the value is larger than the mask
        // we create the patch and record the gap
        if (baseRedLiterals[i] > mask)
        {
            gap = i - prev;
            maxGap = std::max(maxGap, gap);

            // gaps are relative values, so store the previous patched value index
            prev = i;
            gapList[gapIdx++] = gap;

            // extract the most significant bits that are over mask bits
            long patch = ((unsigned long) baseRedLiterals[i]) >> brBits95p;
            patchList[patchIdx++] = patch;

            // strip off the msb to enable sage bit packing
            baseRedLiterals[i] &= mask;
        }
    }

    // align the patch length to the number of entries in gap list
    patchLength = gapIdx;

    // if the element to be patched is the first and only element
    // then the max gap will be 0, 
    // but we need at least one bit to store a 0
    if (maxGap == 0 && patchLength != 0)
    {
        patchGapWidth = 1;
    }
    else
    {
        patchGapWidth = findClosestNumBits(maxGap);
    }

    // special case: if the patch gap is greater than 256, then
    // we need 9 bits to encode the gap. But we only have 3 bits in
    // header to record the gap. To deal with this case, we will save
    // two entries in patch list in the following way
    // 256 gap => 0 for patch value
    // actual gap - 256 => actual patch value
    // We will do the same for gap = 511. If the element to be patched is
    // the last element in the scope then gap will be 511. In this case we
    // will have 3 entries in the patch list in the following way
    // 255 gap => 0 for patch value
    // 255 gap => 0 for patch value
    // 1 gap => actual patch value
    if (patchGapWidth > 8)
    {
        // for gap = 511, we need two extra entries in patch list
        if (maxGap == 511)
        {
            patchLength += 2;
        }
        else
        {
            patchLength += 1;
        }
    }

    // create gap vs patch list
    gapIdx = 0;
    patchIdx = 0;
    for (int i = 0; i < patchLength; i++)
    {
        long g = gapList[gapIdx++];
        long p = patchList[patchIdx++];
        while (g > 255)
        {
            gapVsPatchList[i++] = (255l << patchWidth);
            g -= 255;
        }

        // store patch value in LSBs and gap in MSBs
        // TODO: if we want to encode the values into little endian
        //       shall we modify here?
        gapVsPatchList[i] = (g << patchWidth) | p;
        gapVsPatchListSize = i + 1;
    }
}

void RunLenIntEncoder::writeValues()
{
    if (numLiterals == 0)
    {
        return;
    }
    // the encoding type must be initialized
    assert(encodingType != EncodingType::INIT);
    switch (encodingType)
    {
        case EncodingType::SHORT_REPEAT:
        {
            writeShortRepeatValues();
            break;
        }
        case EncodingType::DIRECT:
        {
            writeDirectValues();
            break;
        }
        case EncodingType::PATCHED_BASE:
        {
            writePatchedBaseValues();
            break;
        }
        case EncodingType::DELTA:
        {
            writeDeltaValues();
            break;
        }
        default:
        {
            // TODO: error case, we should throw error here
            assert(false);
        }
    }
    clear();
}

void RunLenIntEncoder::writeShortRepeatValues()
{
    long repeatVal;
    if (isSigned)
    {
        repeatVal = zigzagEncode(literals[0]);
    }
    else
    {
        repeatVal = literals[0];
    }

    int numBitsRepeatVal = findClosestNumBits(repeatVal);
    int numBytesRepeatVal = numBitsRepeatVal % 8 == 0
                            ? ((unsigned) numBitsRepeatVal) >> 3
                            : (((unsigned) numBitsRepeatVal) >> 3) + 1;
    // -----------------------------------------------------------
    // Header
    // -----------------------------------------------------------
    // short repeat header (1 byte)
    // encoding type (2 bits)
    int header = getOpcode();
    // width of repeating value (3 bits, representing 1~8 bytes)
    header |= ((numBytesRepeatVal - 1) << 3);

    // repeat count (3 bits, 3~10 values)
    fixedRunLength -= Constants::MIN_REPEAT;
    header |= fixedRunLength;

    // write header
    outputStream->put(header);

    // -----------------------------------------------------------
    // Repeating values
    // -----------------------------------------------------------
    // write the repeating values in byte order
    for (int i = numBytesRepeatVal - 1; i >= 0; --i)
    {
        byte b = (byte)((((unsigned long) repeatVal) >> (i * 8)) & 0xff);
        outputStream->put(b);
    }

    fixedRunLength = 0;
}

void RunLenIntEncoder::writeDirectValues()
{
    // write the number of fixed bits required in next 5 bits
    int fb = zzBits100p;
    if (isAlignedBitPacking)
    {
        fb = getClosestAlignedFixedBits(fb);
    }
    int efb = encodingUtils.encodeBitWidth(fb) << 1;

    // adjust variable run length
    variableRunLength -= 1;
    // std::cout << "variableRunLength: " << variableRunLength << std::endl;

    // extract the 9th bit of the run length
    int tailBits = (int) (((unsigned) (variableRunLength & 0x100)) >> 8);
    // std::cout << "tailBits: " << tailBits << std::endl;

    // -----------------------------------------------------------
    // Header
    // -----------------------------------------------------------
    // header (2 bytes)
    //   encoding type (2 bits)
    //   encoded width of values (5 bits, representing 1~64 bits)
    //   length (9 bits, representing 1~512 values)
    byte headerFirstByte = getOpcode() | efb | tailBits;
    byte headerSecondByte = variableRunLength & 0xff;
    // std::cout << "headerFirstByte: " << (int)headerFirstByte << std::endl;

    // write header
    outputStream->put(headerFirstByte);
    outputStream->put(headerSecondByte);
    writeInts(zigzagLiterals, 0, numLiterals, fb);

    variableRunLength = 0;
}

void RunLenIntEncoder::writePatchedBaseValues()
{
    // the number of fixed bits required in next 5 bits
    int fb = brBits95p;
    int efb = encodingUtils.encodeBitWidth(fb) << 1;
    // adjust variable run length
    variableRunLength -= 1;

    // extract the 9th bit of run length
    int tailBits = ((unsigned int) (variableRunLength & 0x100)) >> 8;
    // create the first byte of the header
    int headerFirstByte = getOpcode() | efb | tailBits;
    // the second byte of the header stores the remaining 8 bits of run length
    int headerSecondByte = variableRunLength & 0xff;

    // if the min value is negative, toggle the sign
    bool isNegative = (min < 0);
    min = isNegative ? -min : min;

    // find the number of bytes required for base and shift it by 5 bits to
    // accommodate patch width. The additional bit is used to store the sign of the base value
    int baseWidth = findClosestNumBits(min);
    int baseBytes = baseWidth % 8 == 0 ? baseWidth / 8 : (baseWidth / 8) + 1;
    int bb = (baseBytes - 1) << 5;

    // if the base value if negative, then set MSB to 1
    min = isNegative ? min | (1l << ((baseBytes * 8) - 1)) : min;

    // the third byte contains 3 bits for number of bytes occupied by base
    // and 5 bits for patchWidth
    int headerThirdByte = bb | encodingUtils.encodeBitWidth(patchWidth);
    // the fourth byte contains 3 bits for page gap width and 5 bits for patch length
    int headerFourthByte = (patchGapWidth - 1) << 5 | patchLength;

    // write header
    outputStream->put(headerFirstByte);
    outputStream->put(headerSecondByte);
    outputStream->put(headerThirdByte);
    outputStream->put(headerFourthByte);


    // write the base value using fixed bytes in big endian order
    for (int i = baseBytes - 1; i >= 0; --i)
    {
        byte b = (byte)(((unsigned long) min) >> (i * 8) & 0xff);
        outputStream->put(b);
    }

    // base reduced literals are bit packed
    int closestFixedBits = encodingUtils.getClosestFixedBits(fb);

    writeInts(baseRedLiterals, 0, numLiterals, closestFixedBits);

    // write patch list
    closestFixedBits = encodingUtils.getClosestFixedBits(patchGapWidth + patchWidth);

    writeInts(gapVsPatchList, 0, gapVsPatchListSize, closestFixedBits);

    // reset run length
    variableRunLength = 0;
}


void RunLenIntEncoder::writeDeltaValues()
{
    int len;
    int fb = bitsDeltaMax;
    int efb = 0;

    if (isAlignedBitPacking)
    {
        fb = getClosestAlignedFixedBits(fb);
    }

    if (isFixedDelta)
    {
        // if fixed run length is greater than threshold then it will be fixed
        // delta sequence with delta value 0 else fixed delta sequence with
        // non-zero delta value
        if (fixedRunLength > Constants::MIN_REPEAT)
        {
            // ex. sequence: 2 2 2 2 2 2 2 2
            len = fixedRunLength - 1;
            fixedRunLength = 0;
        }
        else
        {
            // ex. sequence: 4 6 8 10 12 14 16
            len = variableRunLength - 1;
            variableRunLength = 0;
        }
    }
    else
    {
        // fixed width 0 is used for long repeating values.
        // sequences that require only 1 bit to encode will have an additional bit
        if (fb == 1)
        {
            fb = 2;
        }
        efb = encodingUtils.encodeBitWidth(fb);
        efb = efb << 1;
        len = variableRunLength - 1;
        variableRunLength = 0;
    }

    // extract the 9th bit of run length
    int tailBits = ((unsigned) (len & 0x100)) >> 8;

    // header(2 bytes):
    // encoding type(2 bits)
    // encoded width of deltas(5 bits, representing 0 to 64 bits)
    // run length(9 bits, representing 1 to 512 values)
    // create first byte of the header
    int headerFirstByte = getOpcode() | efb | tailBits;

    // second byte of the header stores the remaining 8 bits of runlength
    int headerSecondByte = len & 0xff;

    // write header
    outputStream->put(headerFirstByte);
    outputStream->put(headerSecondByte);

    // store the first value from zigzag literal array
    if (isSigned)
    {
        writeVslong(outputStream, literals[0]);
    }
    else
    {
        writeVulong(outputStream, literals[0]);
    }

    if (isFixedDelta)
    {
        // if delta is fixed then we don't need to store delta blob
        writeVslong(outputStream, fixedDelta);
    }
    else
    {
        // store the first value as delta value using zigzag encoding
        writeVslong(outputStream, adjDeltas[0]);

        // adjacent delta values are bit-packed. The length of adjDeltas array is
        // always one less than the number of literals (delta difference for n
        // elements is n-1). We have already written one element, write the
        // remaining numLiterals - 2 elements here
        writeInts(adjDeltas, 1, numLiterals - 2, fb);
    }
}

void RunLenIntEncoder::writeInts(long *input, int offset, int len, int bitSize)
{
    if (input == nullptr || offset < 0 || len < 1 || bitSize < 1)
    {
        return;
    }

    switch (bitSize)
    {
        case 1:
            encodingUtils.unrolledBitPack1(input, offset, len, outputStream);
            return;
        case 2:
            encodingUtils.unrolledBitPack2(input, offset, len, outputStream);
            return;
        case 4:
            encodingUtils.unrolledBitPack4(input, offset, len, outputStream);
            return;
        case 8:
            encodingUtils.unrolledBitPack8(input, offset, len, outputStream);
            return;
        case 16:
            encodingUtils.unrolledBitPack16(input, offset, len, outputStream);
            return;
        case 24:
            encodingUtils.unrolledBitPack24(input, offset, len, outputStream);
            return;
        case 32:
            encodingUtils.unrolledBitPack32(input, offset, len, outputStream);
            return;
        case 40:
            encodingUtils.unrolledBitPack40(input, offset, len, outputStream);
            return;
        case 48:
            encodingUtils.unrolledBitPack48(input, offset, len, outputStream);
            return;
        case 56:
            encodingUtils.unrolledBitPack56(input, offset, len, outputStream);
            return;
        case 64:
            encodingUtils.unrolledBitPack64(input, offset, len, outputStream);
            return;
        default:
            break;
    }
    // -----------------------------------------------------------
    // if bit size does not match any of above cases
    int bitsLeft = 8;
    byte current = 0;

    // PENDING: different from big endian, we cannot directly 
    //          joint start of the next value with the previous one
    //          using little endian, since the remaining part of the
    //          previous value will be at the higher parts of the encoded bytes.
    for (int i = offset; i < (offset + len); ++i)
    {
        long value = input[i];
        int bitsToWrite = bitSize;
        while (bitsToWrite > bitsLeft)
        {
            // add the bits to the bottom of the current word
            current |= ((unsigned long) value) >> (bitsToWrite - bitsLeft);
            // subtract out the bits we just added
            bitsToWrite -= bitsLeft;
            // zero out the bits above bitsToWrite
            value &= (1L << bitsToWrite) - 1;
            outputStream->putBytes(&current, 1);
            current = 0;
            bitsLeft = 8;
        }
        bitsLeft -= bitsToWrite;
        current |= value << bitsLeft;

        if (bitsLeft == 0)
        {
            outputStream->put(current);
            current = 0;
            bitsLeft = 8;
        }
    }
    // flush
    if (bitsLeft != 8)
    {
        outputStream->put(current);
        current = 0;
        bitsLeft = 8;
    }

}

int RunLenIntEncoder::getOpcode()
{
    return ((int) encodingType) << 6;
}

void RunLenIntEncoder::write(long value)
{
    if (numLiterals == 0)
    {
        initializeLiterals(value);
    }
    else
    {
        // currently only one value so we only need to compare them
        if (numLiterals == 1)
        {
            prevDelta = value - literals[0];
            literals[numLiterals++] = value;

            // if two values are the same, treat them as fixed run else variable run
            if (value == literals[0])
            {
                fixedRunLength = 2;
                variableRunLength = 0;
            }
            else
            {
                fixedRunLength = 0;
                variableRunLength = 2;
            }
        }
            // more than one value
        else
        {
            auto curDelta = value - literals[numLiterals - 1];
            // fixed run
            if (prevDelta == 0 && curDelta == 0)
            {
                literals[numLiterals++] = value;

                // if variable run is not zero, there are repeated values
                // at the end of variable run, keep updating both variable and fixed run len
                if (variableRunLength > 0)
                {
                    // QUESTION: why set fixed to 2 here, min repeat = 3?
                    fixedRunLength = 2;
                }

                fixedRunLength += 1;

                // if fixed run len meets the minimum repeat threshold, and variable len is non-zero
                if (fixedRunLength >= Constants::MIN_REPEAT && variableRunLength > 0)
                {
                    numLiterals -= Constants::MIN_REPEAT;
                    // before entering this branch, last (min_repeat - 1) same values are counted into variable run
                    variableRunLength -= (Constants::MIN_REPEAT - 1);
                    long *tailVals = new long[Constants::MIN_REPEAT];
                    // copy out the current fixed run part
                    // PENDING: can we use memcpy here?
                    std::memcpy(tailVals, literals + numLiterals, Constants::MIN_REPEAT * sizeof(long));
                    // flush the variable run  
                    determineEncoding();
                    writeValues();
                    // shift the tail fixed runs to the start of the buffer
                    memcpy(literals + numLiterals, tailVals, Constants::MIN_REPEAT * sizeof(long));
                    numLiterals += Constants::MIN_REPEAT;
                    delete[] tailVals;

                }

                if (fixedRunLength == Constants::MAX_SCOPE)
                {
                    determineEncoding();
                    writeValues();
                }
            }
                // current values are variable run
            else
            {
                // if fixed run length meets the minimum repeat threshold
                if (fixedRunLength >= Constants::MIN_REPEAT)
                {
                    // if meets the short repeat condition, write values as short repeats
                    if (fixedRunLength <= Constants::MAX_SHORT_REPEAT_LENGTH)
                    {
                        encodingType = EncodingType::SHORT_REPEAT;
                        writeValues();
                    }
                        // if not, use delta encoding
                    else
                    {
                        encodingType = EncodingType::DELTA;
                        isFixedDelta = true;
                        writeValues();
                    }
                }

                // if fixed run length is smaller than the minimum repeat threshold
                // and current value is different from previous one
                // it is a variable run
                if (fixedRunLength > 0 && fixedRunLength < Constants::MIN_REPEAT)
                {
                    if (value != literals[numLiterals - 1])
                    {
                        variableRunLength = fixedRunLength;
                        fixedRunLength = 0;
                    }
                }

                // after writing values, reset the literals
                if (numLiterals == 0)
                {
                    initializeLiterals(value);
                }
                    // keep updating variable run length
                else
                {
                    prevDelta = value - literals[numLiterals - 1];
                    literals[numLiterals++] = value;
                    variableRunLength += 1;

                    // flush variable run if it reaches the max scope
                    if (variableRunLength == Constants::MAX_SCOPE)
                    {
                        determineEncoding();
                        writeValues();
                    }
                }
            }
        }
    }
}

void RunLenIntEncoder::flush()
{
    if (numLiterals != 0)
    {
        if (variableRunLength != 0)
        {
            determineEncoding();
            writeValues();
        }
        else if (fixedRunLength != 0)
        {
            if (fixedRunLength < Constants::MIN_REPEAT)
            {
                variableRunLength = fixedRunLength;
                fixedRunLength = 0;
                determineEncoding();
                writeValues();
            }
            else if (fixedRunLength >= Constants::MIN_REPEAT
                     && fixedRunLength <= Constants::MAX_SHORT_REPEAT_LENGTH)
            {
                encodingType = EncodingType::SHORT_REPEAT;
                writeValues();
            }
            else
            {
                encodingType = EncodingType::DELTA;
                isFixedDelta = true;
                writeValues();
            }
        }
    }
    // In Java version, outputStream does nothing on flush
    // outputStream->flush();
}

void RunLenIntEncoder::initializeLiterals(long value)
{
    assert(numLiterals == 0);
    literals[numLiterals++] = value;
    fixedRunLength = 1;
    variableRunLength = 1;
}

/**
 * @brief reset all variables of encoder
 */
void RunLenIntEncoder::clear()
{
    numLiterals = 0;
    encodingType = EncodingType::INIT;
    prevDelta = 0;
    fixedDelta = 0;
    zzBits90p = 0;
    zzBits100p = 0;
    brBits95p = 0;
    brBits100p = 0;
    bitsDeltaMax = 0;
    patchGapWidth = 0;
    patchLength = 0;
    patchWidth = 0;

    min = 0;
    isFixedDelta = true;
}

// Compute the number of bits required to represent pth percentile value
int RunLenIntEncoder::percentileBits(long *data, int offset, int length, double p)
{
    if ((p > 1.0) || (p <= 0.0))
    {
        return -1;
    }

    int hist[32];
    for (int i = offset; i < (offset + length); ++i)
    {
        // QUESTION: there is calling of getClosestFixedBits in encodeBitWidth function, 
        //           is it redundant here to call it? maybe just count is enough
        //           might not matter
        int idx = encodingUtils.encodeBitWidth(findClosestNumBits(data[i]));
        hist[idx] += 1;
    }

    int perLen = (int) (length * (1.0 - p));

    for (int i = 32 - 1; i >= 0; i--)
    {
        perLen -= hist[i];
        if (perLen < 0)
        {
            return encodingUtils.decodeBitWidth(i);
        }
    }

    return 0;
}

// Count the number of bits required to encode the given value
int RunLenIntEncoder::findClosestNumBits(long value)
{
    int count = 0;
    while (value != 0)
    {
        count++;
        value = ((unsigned long) value) >> 1;
    }
    return encodingUtils.getClosestFixedBits(count);
}

bool RunLenIntEncoder::isSafeSubtract(long left, long right)
{
    // if left and right have the same sign, it is safe to subtract
    // else left should have same sign with (left - right) (no overflow)
    return ((left ^ right) >= 0) || ((left ^ (left - right)) >= 0);
}

int RunLenIntEncoder::getClosestAlignedFixedBits(int n)
{
    if (n == 0 || n == 1)
    {
        return 1;
    }
    else if (n > 1 && n <= 2)
    {
        return 2;
    }
    else if (n > 2 && n <= 4)
    {
        return 4;
    }
    else if (n > 4 && n <= 8)
    {
        return 8;
    }
    else if (n > 8 && n <= 16)
    {
        return 16;
    }
    else if (n > 16 && n <= 24)
    {
        return 24;
    }
    else if (n > 24 && n <= 32)
    {
        return 32;
    }
    else if (n > 32 && n <= 40)
    {
        return 40;
    }
    else if (n > 40 && n <= 48)
    {
        return 48;
    }
    else if (n > 48 && n <= 56)
    {
        return 56;
    }
    else
    {
        return 64;
    }
}

// -----------------------------------------------------------
// Zigzag encode
void RunLenIntEncoder::computeZigZagLiterals()
{
    long zzEncVal = 0;
    if (isSigned)
    {
        for (int i = 0; i < numLiterals; ++i)
        {
            zzEncVal = zigzagEncode(literals[i]);
            zigzagLiterals[i] = zzEncVal;
        }
    }
    else
    {
        for (int i = 0; i < numLiterals; ++i)
        {
            zzEncVal = literals[i];
            zigzagLiterals[i] = zzEncVal;
        }
    }
}

long RunLenIntEncoder::zigzagEncode(long val)
{
    return (val << 1) ^ (val >> 63);
}

void RunLenIntEncoder::writeVulong(std::shared_ptr <ByteBuffer> output, long value)
{
    while (true)
    {
        if ((value & ~0x7f) == 0)
        {
            output->put((byte) value);
            return;
        }
        else
        {
            output->put((byte)(0x80 | (value & 0x7f)));
            value = ((unsigned) value) >> 7;
        }
    }
}

void RunLenIntEncoder::writeVslong(std::shared_ptr <ByteBuffer> output, long value)
{
    writeVulong(output, (static_cast<unsigned long>(value) << 1) ^ (value >> 63));
}