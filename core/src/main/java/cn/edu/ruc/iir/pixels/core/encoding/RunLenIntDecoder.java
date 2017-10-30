package cn.edu.ruc.iir.pixels.core.encoding;

import cn.edu.ruc.iir.pixels.core.Constants;
import cn.edu.ruc.iir.pixels.core.utils.EncodingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

/**
 * pixels
 *
 * @author guodong
 */
public class RunLenIntDecoder extends IntDecoder
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RunLenIntDecoder.class);

    private final InputStream inputStream;
    private final boolean isSigned;
    private final long[] literals = new long[Constants.MAX_SCOPE];
    private final EncodingUtils encodingUtils = new EncodingUtils();

    private boolean isRepeating = false;
    private int used = 0;
    private int numLiterals = 0;

    private RunLenIntEncoder.EncodingType currentEncoding;

    private final static RunLenIntEncoder.EncodingType[] encodings = RunLenIntEncoder.EncodingType.values();

    public RunLenIntDecoder(InputStream inputStream, boolean isSigned)
    {
        this.inputStream = inputStream;
        this.isSigned = isSigned;
    }

    @Override
    public long next() throws IOException
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
    public boolean hasNext() throws IOException
    {
        return used != numLiterals || inputStream.available() > 0;
    }

    private void readValues() throws IOException
    {
        isRepeating = false;
        int firstByte = inputStream.read();
        if (firstByte < 0)
        {
            // todo deal with error
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
                // todo error
                LOGGER.error("Cannot match any supported encoding strategies");
        }
    }

    private void readShortRepeatValues(int firstByte) throws IOException
    {
        int size = (firstByte >>> 3) & 0x07;
        size += 1;

        int len = firstByte & 0x07;
        len += Constants.MIN_REPEAT;

        long val = bytesToLongBE(inputStream, size);

        if (isSigned)
        {
            val = zigzagDecode(val);
        }

        if (numLiterals != 0)
        {
            // todo error
            LOGGER.error("numLiterals is not zero");
        }

        isRepeating = true;
        for (int i = 0; i < len; i++)
        {
            literals[i] = val;
        }
        numLiterals = len;
    }

    private void readDirectValues(int firstByte) throws IOException
    {
        int fbo = (firstByte >>> 1) & 0x1f;
        int fb = encodingUtils.decodeBitWidth(fbo);

        int len = (firstByte & 0x01) << 8;
        len |= inputStream.read();
        len += 1;

        readInts(literals, numLiterals, len, fb, inputStream);
        if (isSigned) {
            for (int i = 0; i < len; i++) {
                literals[numLiterals] = zigzagDecode(literals[numLiterals]);
                numLiterals++;
            }
        }
        else {
            numLiterals += len;
        }
    }

    private void readPatchedBaseValues(int firstByte) throws IOException
    {
        int fbo = (firstByte >>> 1) & 0x1f;
        int fb = encodingUtils.decodeBitWidth(fbo);

        int len = (firstByte & 0x01) << 8;
        len |= inputStream.read();
        len += 1;

        int thirdByte = inputStream.read();
        int bw = (thirdByte >>> 5) & 0x07;
        bw += 1;

        int pwo = thirdByte & 0x1f;
        int pw = encodingUtils.decodeBitWidth(pwo);

        int fourthByte = inputStream.read();
        int pgw = (fourthByte >>> 5) & 0x07;
        pgw += 1;

        int pl = fourthByte & 0x1f;

        long base = bytesToLongBE(inputStream, bw);
        long mask = (1L << ((bw * 8) - 1));
        if ((base & mask) != 0) {
            base = base & ~mask;
            base = -base;
        }

        long[] unpacked = new long[len];
        readInts(unpacked, 0, len, fb, inputStream);

        long[] unpackedPatch = new long[pl];

        if ((pw + pgw) > 64) {
            // todo error
            LOGGER.error("pw add pgw is bigger than 64");
        }
        int bitSize = encodingUtils.getClosestFixedBits(pw + pgw);
        readInts(unpackedPatch, 0, pl, bitSize, inputStream);

        int patchIdx = 0;
        long currGap = 0;
        long currPatch = 0;
        long patchMask = ((1L << pw) - 1);
        currGap = unpackedPatch[patchIdx] >>> pw;
        currPatch = unpackedPatch[patchIdx] & patchMask;
        long actualGap = 0;

        while (currGap == 255 && currPatch == 0) {
            actualGap += 255;
            patchIdx++;
            currGap = unpackedPatch[patchIdx] >>> pw;
            currPatch = unpackedPatch[patchIdx] & patchMask;
        }
        actualGap += currGap;

        for (int i = 0; i < unpacked.length; i++) {
            if (i == actualGap) {
                long patchedVal = unpacked[i] | (currPatch << fb);
                literals[numLiterals++] = base + patchedVal;
                patchIdx++;
                if (patchIdx < pl) {
                    currGap = unpackedPatch[patchIdx] >>> pw;
                    currPatch = unpackedPatch[patchIdx] & patchMask;
                    actualGap = 0;

                    while (currGap == 255 && currPatch == 0) {
                        actualGap += 255;
                        patchIdx++;
                        currGap = unpackedPatch[patchIdx] >>> pw;
                        currPatch = unpackedPatch[patchIdx] & patchMask;
                    }
                    actualGap += currGap;
                    actualGap += i;
                }
            }
            else {
                literals[numLiterals++] = base + unpacked[i];
            }
        }
    }

    private void readDeltaValues(int firstByte) throws IOException
    {
        int fb = (firstByte >>> 1) & 0x1f;
        if (fb != 0) {
            fb = encodingUtils.decodeBitWidth(fb);
        }

        int len = (firstByte & 0x01) << 8;
        len |= inputStream.read();

        long firstVal = 0;
        if (isSigned) {
            firstVal = readVslong(inputStream);
        }
        else {
            firstVal = readVulong(inputStream);
        }

        long prevVal = firstVal;
        literals[numLiterals++] = firstVal;

        if (fb == 0) {
            long fd = readVslong(inputStream);
            if (fd == 0) {
                isRepeating = true;
                assert numLiterals == 1;
                Arrays.fill(literals, numLiterals, numLiterals + len, literals[0]);
                numLiterals += len;
            }
            else {
                for (int i = 0; i < len; i++) {
                    literals[numLiterals++] = literals[numLiterals - 2] + fd;
                }
            }
        }
        else {
            long deltaBase = readVslong(inputStream);
            literals[numLiterals++] = firstVal + deltaBase;
            prevVal = literals[numLiterals - 1];
            len -= 1;

            readInts(literals, numLiterals, len, fb, inputStream);
            while (len > 0) {
                if (deltaBase < 0) {
                    literals[numLiterals] = prevVal - literals[numLiterals];
                }
                else {
                    literals[numLiterals] = prevVal + literals[numLiterals];
                }
                prevVal = literals[numLiterals];
                len--;
                numLiterals++;
            }
        }
    }

    private void readInts(long[] buffer, int offset, int len, int bitSize,
                         InputStream input) throws IOException {
        int bitsLeft = 0;
        int current = 0;

        switch (bitSize) {
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

        for(int i = offset; i < (offset + len); i++) {
            long result = 0;
            int bitsLeftToRead = bitSize;
            while (bitsLeftToRead > bitsLeft) {
                result <<= bitsLeft;
                result |= current & ((1 << bitsLeft) - 1);
                bitsLeftToRead -= bitsLeft;
                current = input.read();
                bitsLeft = 8;
            }

            // handle the left over bits
            if (bitsLeftToRead > 0) {
                result <<= bitsLeftToRead;
                bitsLeft -= bitsLeftToRead;
                result |= (current >> bitsLeft) & ((1 << bitsLeftToRead) - 1);
            }
            buffer[i] = result;
        }
    }

    private long bytesToLongBE(InputStream input, int n) throws IOException
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

    private long zigzagDecode(long val)
    {
        return (val << 1) ^ (val >> 63);
    }

    private long readVulong(InputStream in) throws IOException {
        long result = 0;
        long b;
        int offset = 0;
        do {
            b = in.read();
            if (b == -1) {
                throw new EOFException("Reading Vulong past EOF");
            }
            result |= (0x7f & b) << offset;
            offset += 7;
        } while (b >= 0x80);
        return result;
    }

    private long readVslong(InputStream in) throws IOException {
        long result = readVulong(in);
        return (result >>> 1) ^ -(result & 1);
    }
}
