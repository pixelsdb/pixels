/*
 * Copyright 2017-2019 PixelsDB.
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
 * License along with Foobar.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.core.encoding;

import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.core.utils.EncodingUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author guodong
 */
public class RunLenIntDecoder
        extends IntDecoder
{
    private static final Logger LOGGER = LogManager.getLogger(RunLenIntDecoder.class);

    private final InputStream inputStream;
    private final boolean isSigned;
    private final long[] literals = new long[Constants.MAX_SCOPE];
    private final EncodingUtils encodingUtils = new EncodingUtils();

    private boolean isRepeating = false;
    private int used = 0;
    private int numLiterals = 0;

    private RunLenIntEncoder.EncodingType currentEncoding;

    private final static RunLenIntEncoder.EncodingType[] encodings =
            RunLenIntEncoder.EncodingType.values();

    public RunLenIntDecoder(InputStream inputStream, boolean isSigned)
    {
        this.inputStream = inputStream;
        this.isSigned = isSigned;
    }

    @Override
    public long next()
            throws IOException
    {
        long result;
        if (used == numLiterals)
        {
            numLiterals = 0;
            used = 0;
            readValues();
        }
        result = literals[used++];
        return result;
    }

    @Override
    public boolean hasNext()
            throws IOException
    {
        return used != numLiterals || inputStream.available() > 0;
    }

    private void readValues()
            throws IOException
    {
        // read the first 2 bits and determine the encoding type
        isRepeating = false;
        int firstByte = inputStream.read();
        if (firstByte < 0)
        {
            LOGGER.error("error first byte is negative");
            used = numLiterals = 0;
            return;
        }
        currentEncoding = encodings[(firstByte >>> 6) & 0x03];
        switch (currentEncoding)
        {
            case SHORT_REPEAT:
                readShortRepeatValues(firstByte);
                break;
            case DIRECT:
                readDirectValues(firstByte);
                break;
            case PATCHED_BASE:
                readPatchedBaseValues(firstByte);
                break;
            case DELTA:
                readDeltaValues(firstByte);
                break;
            default:
                LOGGER.error("Cannot match any supported encoding strategies");
        }
    }

    private void readShortRepeatValues(int firstByte)
            throws IOException
    {
        // read the number of bytes occupied by the value
        int size = (firstByte >>> 3) & 0x07;
        // number of bytes are one off
        size += 1;

        // read the run length
        int len = firstByte & 0x07;
        // run length values are stored only after MIN_REPEAT value is met
        len += Constants.MIN_REPEAT;

        // read the repeated value which is stored using fixed bytes
        long val = bytesToLongBE(inputStream, size);

        if (isSigned)
        {
            val = zigzagDecode(val);
        }

        if (numLiterals != 0)
        {
            // currently this always holds, which makes peekNextAvailLength simpler.
            // if this changes, peekNextAvailLength should be adjusted accordingly.
            LOGGER.error("numLiterals is not zero");
            return;
        }

        // repeat the value for length times
        isRepeating = true;
        // TODO: this is not so useful and V1 reader doesn't do that. Fix? Same if delta == 0
        for (int i = 0; i < len; i++)
        {
            literals[i] = val;
        }
        numLiterals = len;
    }

    private void readDirectValues(int firstByte)
            throws IOException
    {
        // extract the number of fixed bits
        int fbo = (firstByte >>> 1) & 0x1f;
        int fb = encodingUtils.decodeBitWidth(fbo);

        // extract run length
        int len = (firstByte & 0x01) << 8;
        len |= inputStream.read();
        // runs are one off
        len += 1;

        // write the unpacked values and zigzag decode to result buffer
        readInts(literals, numLiterals, len, fb, inputStream);
        if (isSigned)
        {
            for (int i = 0; i < len; i++)
            {
                literals[numLiterals] = zigzagDecode(literals[numLiterals]);
                numLiterals++;
            }
        }
        else
        {
            numLiterals += len;
        }
    }

    private void readPatchedBaseValues(int firstByte)
            throws IOException
    {
        // extract the number of fixed bits
        int fbo = (firstByte >>> 1) & 0x1f;
        int fb = encodingUtils.decodeBitWidth(fbo);

        // extract the run length of data blob
        int len = (firstByte & 0x01) << 8;
        len |= inputStream.read();
        // runs are always one off
        len += 1;

        // extract the number of bytes occupied by base
        int thirdByte = inputStream.read();
        int bw = (thirdByte >>> 5) & 0x07;
        // base width is one off
        bw += 1;

        // extract patch width
        int pwo = thirdByte & 0x1f;
        int pw = encodingUtils.decodeBitWidth(pwo);

        // read fourth byte and extract patch gap width
        int fourthByte = inputStream.read();
        int pgw = (fourthByte >>> 5) & 0x07;
        // patch gao width is one off
        pgw += 1;

        // extract the length of the patch list
        int pl = fourthByte & 0x1f;

        // read the next base width number of bytes to extract base value
        long base = bytesToLongBE(inputStream, bw);
        long mask = (1L << ((bw * 8) - 1));
        // if MSB of base value is 1 then base is negative value else positive
        if ((base & mask) != 0)
        {
            base = base & ~mask;
            base = -base;
        }

        // unpack the data blob
        long[] unpacked = new long[len];
        readInts(unpacked, 0, len, fb, inputStream);

        // unpack the patch blob
        long[] unpackedPatch = new long[pl];

        if ((pw + pgw) > 64)
        {
            LOGGER.error("pw add pgw is bigger than 64");
            return;
        }
        int bitSize = encodingUtils.getClosestFixedBits(pw + pgw);
        readInts(unpackedPatch, 0, pl, bitSize, inputStream);

        // apply the patch directly when adding the packed data
        int patchIdx = 0;
        long currGap = 0;
        long currPatch = 0;
        long patchMask = ((1L << pw) - 1);
        currGap = unpackedPatch[patchIdx] >>> pw;
        currPatch = unpackedPatch[patchIdx] & patchMask;
        long actualGap = 0;

        // special case: gap is greater than 255 then patch value will be 0
        // if gap is smaller or equal than 255 then patch value cannot be 0
        while (currGap == 255 && currPatch == 0)
        {
            actualGap += 255;
            patchIdx++;
            currGap = unpackedPatch[patchIdx] >>> pw;
            currPatch = unpackedPatch[patchIdx] & patchMask;
        }
        // and the left over gap
        actualGap += currGap;

        // unpack data blob, patch it (if required), add base to get final result
        for (int i = 0; i < unpacked.length; i++)
        {
            if (i == actualGap)
            {
                // extract the patch value
                long patchedVal = unpacked[i] | (currPatch << fb);
                // add base to patched value
                literals[numLiterals++] = base + patchedVal;
                // increment the patch to point to next entry in patch list
                patchIdx++;
                if (patchIdx < pl)
                {
                    // read the next gap and patch
                    currGap = unpackedPatch[patchIdx] >>> pw;
                    currPatch = unpackedPatch[patchIdx] & patchMask;
                    actualGap = 0;

                    // special case: gap is grater than 255 then patch will be 0
                    // if gap is smaller or equal than 255 then patch cannot be 0
                    while (currGap == 255 && currPatch == 0)
                    {
                        actualGap += 255;
                        patchIdx++;
                        currGap = unpackedPatch[patchIdx] >>> pw;
                        currPatch = unpackedPatch[patchIdx] & patchMask;
                    }
                    // add the left over gap
                    actualGap += currGap;

                    // next gap is relative to the current gap
                    actualGap += i;
                }
            }
            else
            {
                // no patching required. add base to unpacked value to get final value
                literals[numLiterals++] = base + unpacked[i];
            }
        }
    }

    private void readDeltaValues(int firstByte)
            throws IOException
    {
        // extract the number of fixed bits
        int fb = (firstByte >>> 1) & 0x1f;
        if (fb != 0)
        {
            fb = encodingUtils.decodeBitWidth(fb);
        }

        // extract the blob run length
        int len = (firstByte & 0x01) << 8;
        len |= inputStream.read();

        // read the first value stored as vint
        long firstVal = 0;
        if (isSigned)
        {
            firstVal = readVslong(inputStream);
        }
        else
        {
            firstVal = readVulong(inputStream);
        }

        // store first value to result buffer
        long prevVal = firstVal;
        literals[numLiterals++] = firstVal;

        // if fixed bits is 0 then all values have fixed delta
        if (fb == 0)
        {
            // read the fixed delta value stored as vint (deltas
            // can be negative even if all number are positive)
            long fd = readVslong(inputStream);
            if (fd == 0)
            {
                isRepeating = true;
                checkArgument(numLiterals == 1, "num literals is not equal to 1");
                Arrays.fill(literals, numLiterals, numLiterals + len, literals[0]);
                numLiterals += len;
            }
            else
            {
                // add fixed deltas to adjacent values
                for (int i = 0; i < len; i++)
                {
                    literals[numLiterals++] = literals[numLiterals - 2] + fd;
                }
            }
        }
        else
        {
            long deltaBase = readVslong(inputStream);
            // add delta base and first value
            literals[numLiterals++] = firstVal + deltaBase;
            prevVal = literals[numLiterals - 1];
            len -= 1;

            // write the unpacked values, add it to previous values and store final
            // value to result buffer. if the delta base value is negative then it
            // is a decreasing sequence else an increasing sequence
            readInts(literals, numLiterals, len, fb, inputStream);
            while (len > 0)
            {
                if (deltaBase < 0)
                {
                    literals[numLiterals] = prevVal - literals[numLiterals];
                }
                else
                {
                    literals[numLiterals] = prevVal + literals[numLiterals];
                }
                prevVal = literals[numLiterals];
                len--;
                numLiterals++;
            }
        }
    }

    /**
     * Read bitpacked integers from input stream
     */
    private void readInts(long[] buffer, int offset, int len, int bitSize,
                          InputStream input)
            throws IOException
    {
        int bitsLeft = 0;
        int current = 0;

        switch (bitSize)
        {
            case 1:
                encodingUtils.unrolledUnPack1(buffer, offset, len, input);
                return;
            case 2:
                encodingUtils.unrolledUnPack2(buffer, offset, len, input);
                return;
            case 4:
                encodingUtils.unrolledUnPack4(buffer, offset, len, input);
                return;
            case 8:
                encodingUtils.unrolledUnPack8(buffer, offset, len, input);
                return;
            case 16:
                encodingUtils.unrolledUnPack16(buffer, offset, len, input);
                return;
            case 24:
                encodingUtils.unrolledUnPack24(buffer, offset, len, input);
                return;
            case 32:
                encodingUtils.unrolledUnPack32(buffer, offset, len, input);
                return;
            case 40:
                encodingUtils.unrolledUnPack40(buffer, offset, len, input);
                return;
            case 48:
                encodingUtils.unrolledUnPack48(buffer, offset, len, input);
                return;
            case 56:
                encodingUtils.unrolledUnPack56(buffer, offset, len, input);
                return;
            case 64:
                encodingUtils.unrolledUnPack64(buffer, offset, len, input);
                return;
            default:
                break;
        }

        for (int i = offset; i < (offset + len); i++)
        {
            long result = 0;
            int bitsLeftToRead = bitSize;
            while (bitsLeftToRead > bitsLeft)
            {
                result <<= bitsLeft;
                result |= current & ((1 << bitsLeft) - 1);
                bitsLeftToRead -= bitsLeft;
                current = input.read();
                bitsLeft = 8;
            }

            // handle the left over bits
            if (bitsLeftToRead > 0)
            {
                result <<= bitsLeftToRead;
                bitsLeft -= bitsLeftToRead;
                result |= (current >> bitsLeft) & ((1 << bitsLeftToRead) - 1);
            }
            buffer[i] = result;
        }
    }

    private long bytesToLongBE(InputStream input, int n)
            throws IOException
    {
        long out = 0;
        long val = 0;
        while (n > 0)
        {
            n--;
            // store it in a long and then shift else integer overflow will occur
            val = input.read();
            out |= (val << (n * 8));
        }
        return out;
    }

    /**
     * zigzag decode given value
     */
    private long zigzagDecode(long val)
    {
        return (val >>> 1) ^ -(val & 1);
    }

    private long readVulong(InputStream in)
            throws IOException
    {
        long result = 0;
        long b;
        int offset = 0;
        do
        {
            b = in.read();
            if (b == -1)
            {
                throw new EOFException("Reading Vulong past EOF");
            }
            result |= (0x7f & b) << offset;
            offset += 7;
        } while (b >= 0x80);
        return result;
    }

    private long readVslong(InputStream in)
            throws IOException
    {
        long result = readVulong(in);
        return (result >>> 1) ^ -(result & 1);
    }
}
