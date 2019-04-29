package cn.edu.ruc.iir.pixels.core.encoding;

import cn.edu.ruc.iir.pixels.common.utils.Constants;
import cn.edu.ruc.iir.pixels.core.utils.EncodingUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * pixels run length encoding
 * there are four kinds of encodings: SHORT_REPEAT, DIRECT, PATCHED_BASE, DELTA.
 * More details can be found at https://orc.apache.org/docs/run-length.html
 *
 * @author guodong
 */
public class RunLenIntEncoder
        extends Encoder
{
    private EncodingType encodingType;
    private int numLiterals;
    private int fixedRunLength = 0;
    private int variableRunLength = 0;
    private long prevDelta = 0L;
    private long fixedDelta;
    private int bitsDeltaMax;
    private int patchWidth;
    private int patchGapWidth;
    private int patchLength;
    private long[] gapVsPatchList;
    private boolean isFixedDelta;
    private boolean isSigned;
    private boolean isAlignedBitpacking;

    private int zzBits90p;
    private int zzBits100p;
    private int brBits95p;
    private int brBits100p;

    private long min;

    private final long[] literals = new long[Constants.MAX_SCOPE];
    private final long[] zigzagLiterals = new long[Constants.MAX_SCOPE];
    private final long[] baseRedLiterals = new long[Constants.MAX_SCOPE];
    private final long[] adjDeltas = new long[Constants.MAX_SCOPE];

    private ByteArrayOutputStream outputStream;
    private EncodingUtils encodingUtils;

    public enum EncodingType
    {
        SHORT_REPEAT, DIRECT, PATCHED_BASE, DELTA
    }

    public RunLenIntEncoder()
    {
        this(true, true);
    }

    public RunLenIntEncoder(boolean isSigned, boolean isAlignedBitpacking)
    {
        this.isSigned = isSigned;
        this.isAlignedBitpacking = isAlignedBitpacking;
        this.outputStream = new ByteArrayOutputStream();
        this.encodingUtils = new EncodingUtils();
        clear();
    }

    public byte[] encode(long[] values, long offset, long length)
            throws IOException
    {
        for (int i = 0; i < length; i++)
        {
            this.write(values[i]);
        }
        flush();
        byte[] result = outputStream.toByteArray();
        outputStream.reset();
        return result;
    }

    @Override
    public byte[] encode(long[] values)
            throws IOException
    {
        return encode(values, 0, values.length);
    }

    @Override
    public void close()
            throws IOException
    {
        gapVsPatchList = null;
        outputStream.close();
    }

    private void determineEncoding()
    {
        // compute zigzag values for DIRECT encoding if we decide to
        // break early for delta overflows or for shorter runs
        computeZigZagLiterals();
        zzBits100p = percentileBits(zigzagLiterals, 0, numLiterals, 1.0);

        // not a big win for shorter runs to determine encoding
        if (numLiterals <= Constants.MIN_REPEAT)
        {
            encodingType = EncodingType.DIRECT;
            return;
        }

        // DELTA encoding check

        // variables for identifying monotonic sequences
        boolean isIncreasing = true;
        boolean isDecreasing = true;
        this.isFixedDelta = true;

        this.min = literals[0];
        long max = literals[0];
        final long initialDelta = literals[1] - literals[0];
        long currDelta = 0;
        long deltaMax = 0;
        this.adjDeltas[0] = initialDelta;

        for (int i = 1; i < numLiterals; i++)
        {
            final long l1 = literals[i];
            final long l0 = literals[i - 1];
            currDelta = l1 - l0;
            min = Math.min(min, l1);
            max = Math.max(max, l1);

            isIncreasing &= (l0 <= l1);
            isDecreasing &= (l0 >= l1);

            isFixedDelta &= (currDelta == initialDelta);
            if (i > 1)
            {
                adjDeltas[i - 1] = Math.abs(currDelta);
                deltaMax = Math.max(deltaMax, adjDeltas[i - 1]);
            }
        }

        // its faster to exit under delta overflow condition without checking for
        // PATCHED_BASE condition as encoding using DIRECT is faster and has less
        // overhead than PATCHED_BASE
        if (!isSafeSubtract(max, min))
        {
            encodingType = EncodingType.DIRECT;
            return;
        }

        // invariant - subtracting any number from any other in the literals after
        // this point won't overflow

        // if min is equal to max then the delta is 0, this condition happens for
        // fixed values run >10 which cannot be encoded with SHORT_REPEAT
        if (min == max)
        {
            checkArgument(isFixedDelta, min + "==" + max + ", isFixedDelta cannot be false");
            checkArgument(currDelta == 0, min + "==" + max + ", currDelta should be zero");
            fixedDelta = 0;
            encodingType = EncodingType.DELTA;
            return;
        }

        if (isFixedDelta)
        {
            checkArgument(currDelta == initialDelta,
                          "currDelta should be equal to initalDelta for fixed delta encoding");
            encodingType = EncodingType.DELTA;
            fixedDelta = currDelta;
            return;
        }

        // if initialDelta is 0 then we cannot delta encode as we cannot identify
        // the sign of deltas (increasing or decreasing)
        if (initialDelta != 0)
        {
            // stores the number of bits required for packing delta blob in
            // delta encoding
            bitsDeltaMax = findClosestNumBits(deltaMax);

            // monotonic condition
            if (isIncreasing || isDecreasing)
            {
                encodingType = EncodingType.DELTA;
                return;
            }
        }

        // PATCHED_BASE encoding check

        // percentile values are computed for the zigzag encoded values. if the
        // number of bit requirement between 90th and 100th percentile varies
        // beyond a threshold then we need to patch the values. if the variation
        // is not significant then we can use direct encoding

        zzBits90p = percentileBits(zigzagLiterals, 0, numLiterals, 0.9);
        int diffBitsLH = zzBits100p - zzBits90p;

        // if the difference between 90th percentile and 100th percentile fixed
        // bits is > 1 then we need patch the values
        if (diffBitsLH > 1)
        {

            // patching is done only on base reduced values.
            // remove base from literals
            for (int i = 0; i < numLiterals; i++)
            {
                baseRedLiterals[i] = literals[i] - min;
            }

            // 95th percentile width is used to determine max allowed value
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
            if ((brBits100p - brBits95p) != 0)
            {
                encodingType = EncodingType.PATCHED_BASE;
                preparePatchedBlob();
            }
            else
            {
                encodingType = EncodingType.DIRECT;
            }
        }
        else
        {
            // if difference in bits between 95th percentile and 100th percentile is
            // 0, then patch length will become 0. Hence we will fallback to direct
            encodingType = EncodingType.DIRECT;
        }
    }

    private void preparePatchedBlob()
    {
        // mask will be max value beyond which patch will be generated
        long mask = (1L << brBits95p) - 1;

        // since we are considering only 95 percentile, the size of gap and
        // patch array can contain only be 5% values
        patchLength = (int) Math.ceil((numLiterals * 0.05));

        int[] gapList = new int[patchLength];
        long[] patchList = new long[patchLength];

        // #bit for patch
        patchWidth = brBits100p - brBits95p;
        patchWidth = encodingUtils.getClosestFixedBits(patchWidth);

        // if patch bit requirement is 64 then it will not possible to pack
        // gap and patch together in a long. To make sure gap and patch can be
        // packed together adjust the patch width
        if (patchWidth == 64)
        {
            patchWidth = 56;
            brBits95p = 8;
            mask = (1L << brBits95p) - 1;
        }

        int gapIdx = 0;
        int patchIdx = 0;
        int prev = 0;
        int gap;
        int maxGap = 0;

        for (int i = 0; i < numLiterals; i++)
        {
            // if value is above mask then create the patch and record the gap
            if (baseRedLiterals[i] > mask)
            {
                gap = i - prev;
                if (gap > maxGap)
                {
                    maxGap = gap;
                }

                // gaps are relative, so store the previous patched value index
                prev = i;
                gapList[gapIdx++] = gap;

                // extract the most significant bits that are over mask bits
                long patch = baseRedLiterals[i] >>> brBits95p;
                patchList[patchIdx++] = patch;

                // strip off the MSB to enable safe bit packing
                baseRedLiterals[i] &= mask;
            }
        }

        // adjust the patch length to number of entries in gap list
        patchLength = gapIdx;

        // if the element to be patched is the first and only element then
        // max gap will be 0, but to store the gap as 0 we need atleast 1 bit
        if (maxGap == 0 && patchLength != 0)
        {
            patchGapWidth = 1;
        }
        else
        {
            patchGapWidth = findClosestNumBits(maxGap);
        }

        // special case: if the patch gap width is greater than 256, then
        // we need 9 bits to encode the gap width. But we only have 3 bits in
        // header to record the gap width. To deal with this case, we will save
        // two entries in patch list in the following way
        // 256 gap width => 0 for patch value
        // actual gap - 256 => actual patch value
        // We will do the same for gap width = 511. If the element to be patched is
        // the last element in the scope then gap width will be 511. In this case we
        // will have 3 entries in the patch list in the following way
        // 255 gap width => 0 for patch value
        // 255 gap width => 0 for patch value
        // 1 gap width => actual patch value
        if (patchGapWidth > 8)
        {
            patchGapWidth = 8;
            // for gap = 511, we need two additional entries in patch list
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
        gapVsPatchList = new long[patchLength];
        for (int i = 0; i < patchLength; i++)
        {
            long g = gapList[gapIdx++];
            long p = patchList[patchIdx++];
            while (g > 255)
            {
                gapVsPatchList[i++] = (255L << patchWidth);
                g -= 255;
            }

            // store patch value in LSBs and gap in MSBs
            gapVsPatchList[i] = (g << patchWidth) | p;
        }
    }

    private void writeValues()
            throws IOException
    {
        if (numLiterals != 0)
        {
            if (encodingType.equals(EncodingType.SHORT_REPEAT))
            {
                writeShortRepeatValues();
            }
            else if (encodingType.equals(EncodingType.DIRECT))
            {
                writeDirectValues();
            }
            else if (encodingType.equals(EncodingType.PATCHED_BASE))
            {
                writePatchedBaseValues();
            }
            else
            {
                writeDeltaValues();
            }

            // clear all the variables
            clear();
        }
    }

    private void write(long value)
            throws IOException
    {
        if (numLiterals == 0)
        {
            initializeLiterals(value);
        }
        else
        {
            if (numLiterals == 1)
            {
                prevDelta = value - literals[0];
                literals[numLiterals++] = value;
                // if both values are same then treated as fixed run else variable run
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
            else
            {
                long curDelta = value - literals[numLiterals - 1];
                // current values are fixed delta run
                if (prevDelta == 0 && curDelta == 0)
                {
                    literals[numLiterals++] = value;
                    // if variable run is non-zero, then we are seeing repeating values at
                    // the end of variable run in which case keep updating variable and fixed runs
                    if (variableRunLength > 0)
                    {
                        fixedRunLength = 2;
                    }
                    fixedRunLength += 1;

                    // if fixed run meets the minimum repeat condition and if variable run is non-zero,
                    // then flush the variable run and shift the tail fixed runs to the start of the buffer
                    if (fixedRunLength >= Constants.MIN_REPEAT && variableRunLength > 0)
                    {
                        numLiterals -= Constants.MIN_REPEAT;
                        variableRunLength -= Constants.MIN_REPEAT - 1;
                        long[] tailVals = new long[Constants.MIN_REPEAT];
                        System.arraycopy(literals, numLiterals, tailVals, 0, Constants.MIN_REPEAT);

                        determineEncoding();
                        writeValues();

                        for (long l : tailVals)
                        {
                            literals[numLiterals++] = l;
                        }
                    }

                    if (fixedRunLength == Constants.MAX_SCOPE)
                    {
                        determineEncoding();
                        writeValues();
                    }
                }
                // current values are variable delta run
                else
                {
                    // if fixed run length is non-zero and if it satisfies the minimum repeat
                    // and the short repeat condition, then write the values as short repeats
                    // else use delta encoding
                    if (fixedRunLength >= Constants.MIN_REPEAT)
                    {
                        if (fixedRunLength <= Constants.MAX_SHORT_REPEAT_LENGTH)
                        {
                            encodingType = EncodingType.SHORT_REPEAT;
                            writeValues();
                        }
                        else
                        {
                            encodingType = EncodingType.DELTA;
                            isFixedDelta = true;
                            writeValues();
                        }
                    }

                    // if fixed run length is smaller than MIN_REPEAT
                    // and current value is different from previous
                    // then treat it as variable run
                    if (fixedRunLength > 0 && fixedRunLength < Constants.MIN_REPEAT)
                    {
                        if (value != literals[numLiterals - 1])
                        {
                            variableRunLength = fixedRunLength;
                            fixedRunLength = 0;
                        }
                    }

                    // if done writing values, re-initialize value literals
                    if (numLiterals == 0)
                    {
                        initializeLiterals(value);
                    }
                    // keep updating variable run lengths
                    else
                    {
                        prevDelta = value - literals[numLiterals - 1];
                        literals[numLiterals++] = value;
                        variableRunLength += 1;

                        // if variable run length reach the max scope, write it
                        if (variableRunLength == Constants.MAX_SCOPE)
                        {
                            determineEncoding();
                            writeValues();
                        }
                    }
                }
            }
        }
    }

    private void flush()
            throws IOException
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
                if (fixedRunLength < Constants.MIN_REPEAT)
                {
                    variableRunLength = fixedRunLength;
                    fixedRunLength = 0;
                    determineEncoding();
                    writeValues();
                }
                else if (fixedRunLength >= Constants.MIN_REPEAT
                        && fixedRunLength <= Constants.MAX_SHORT_REPEAT_LENGTH)
                {
                    encodingType = EncodingType.SHORT_REPEAT;
                    writeValues();
                }
                else
                {
                    encodingType = EncodingType.DELTA;
                    isFixedDelta = true;
                    writeValues();
                }
            }
        }
        outputStream.flush();
    }

    private void writeShortRepeatValues()
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

        final int numBitsRepeatVal = findClosestNumBits(repeatVal);
        final int numBytesRepeatVal = numBitsRepeatVal % 8 == 0 ? numBitsRepeatVal >>> 3
                : (numBitsRepeatVal >>> 3) + 1;

        // short repeat header(1 byte)
        //   encoding type(2 bits),
        //   width of repeating value(3 bits, representing 1 to 8 bytes,
        //   repeat count(3 bytes, 3 to 10 values)
        int header = getOpcode();
        header |= ((numBytesRepeatVal - 1) << 3);
        fixedRunLength -= Constants.MIN_REPEAT;
        header |= fixedRunLength;

        // write header
        outputStream.write(header);

        // write the repeating values in big endian byte order
        for (int i = numBytesRepeatVal - 1; i >= 0; i--)
        {
            int b = (int) ((repeatVal >>> (i * 8)) & 0xff);
            outputStream.write(b);
        }

        fixedRunLength = 0;
    }

    private void writeDirectValues()
            throws IOException
    {
        // write the number of fixed bits required in next 5 bits
        int fb = zzBits100p;

        if (isAlignedBitpacking)
        {
            fb = getClosestAlignedFixedBits(fb);
        }

        final int efb = encodingUtils.encodeBitWidth(fb) << 1;

        // adjust variable run length
        variableRunLength -= 1;

        // extract the 9th bit of run length
        final int tailBits = (variableRunLength & 0x100) >>> 8;

        // header(2 bytes):
        //   encoding type(2 bits)
        //   encoded width of values(5 bits, representing 1 to 64 bits)
        //   length(9 bits, representing 1 to 512 values)
        final int headerFirstByte = getOpcode() | efb | tailBits;
        final int headerSecondByte = variableRunLength & 0xff;

        // write header
        outputStream.write(headerFirstByte);
        outputStream.write(headerSecondByte);

        writeInts(zigzagLiterals, 0, numLiterals, fb);

        // reset run length
        variableRunLength = 0;
    }

    private void writePatchedBaseValues()
            throws IOException
    {
        // NOTE: Aligned bit packing cannot be applied for PATCHED_BASE encoding
        // because patch is applied to MSB bits. For example: If fixed bit width of
        // base value is 7 bits and if patch is 3 bits, the actual value is
        // constructed by shifting the patch to left by 7 positions.
        // actual_value = patch << 7 | base_value
        // So, if we align base_value then actual_value can not be reconstructed.

        // write the number of fixed bits required in next 5 bits
        final int fb = brBits95p;
        final int efb = encodingUtils.encodeBitWidth(fb) << 1;
        // adjust variable run length, they are one off
        variableRunLength -= 1;

        // extract the 9th bit of run length
        final int tailBits = (variableRunLength & 0x100) >>> 8;
        // create first byte of the header
        final int headerFirstByte = getOpcode() | efb | tailBits;
        // second byte of the header stores the remaining 8 bits of run length
        final int headerSecondByte = variableRunLength & 0xff;

        // if the min value if negative, toggle the sign
        final boolean isNegative = min < 0;
        if (isNegative)
        {
            min = -min;
        }

        // find the number of bytes required for base and shift it by 5 bits to
        // accommodate patch width. The additional bit is used to store the sign of the base value
        final int baseWidth = findClosestNumBits(min) + 1;
        final int baseBytes = baseWidth % 8 == 0 ? baseWidth / 8 : (baseWidth / 8) + 1;
        final int bb = (baseBytes - 1) << 5;

        // if the base value if negative, then set MSB to 1
        if (isNegative)
        {
            min |= (1L << ((baseBytes * 8) - 1));
        }

        // third byte contains 3 bits for number of bytes occupied by base
        // and 5 bits for patchWidth
        final int headerThirdByte = bb | encodingUtils.encodeBitWidth(patchWidth);
        // fourth byte contains 3 bits for page gap width and 5 bits for patch length
        final int headerFourthByte = (patchGapWidth - 1) << 5 | patchLength;

        // write header
        outputStream.write(headerFirstByte);
        outputStream.write(headerSecondByte);
        outputStream.write(headerThirdByte);
        outputStream.write(headerFourthByte);

        // write the base value using fixed bytes in big endian order
        for (int i = baseBytes - 1; i >= 0; i--)
        {
            byte b = (byte) ((min >>> (i * 8)) & 0xff);
            outputStream.write(b);
        }

        // base reduced literals are bit packed
        int closestFixedBits = encodingUtils.getClosestFixedBits(fb);

        writeInts(baseRedLiterals, 0, numLiterals, closestFixedBits);

        // write patch list
        closestFixedBits = encodingUtils.getClosestFixedBits(patchGapWidth + patchWidth);

        writeInts(gapVsPatchList, 0, gapVsPatchList.length, closestFixedBits);

        // reset run length
        variableRunLength = 0;
    }

    private void writeDeltaValues()
            throws IOException
    {
        int len;
        int fb = bitsDeltaMax;
        int efb = 0;

        if (isAlignedBitpacking)
        {
            fb = getClosestAlignedFixedBits(fb);
        }

        if (isFixedDelta)
        {
            // if fixed run length is greater than threshold then it will be fixed
            // delta sequence with delta value 0 else fixed delta sequence with
            // non-zero delta value
            if (fixedRunLength > Constants.MIN_REPEAT)
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
        final int tailBits = (len & 0x100) >>> 8;

        // header(2 bytes):
        // encoding type(2 bits)
        // encoded width of deltas(5 bits, representing 0 to 64 bits)
        // run length(9 bits, representing 1 to 512 values)
        // create first byte of the header
        final int headerFirstByte = getOpcode() | efb | tailBits;

        // second byte of the header stores the remaining 8 bits of runlength
        final int headerSecondByte = len & 0xff;

        // write header
        outputStream.write(headerFirstByte);
        outputStream.write(headerSecondByte);

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

            // adjacent delta values are bit packed. The length of adjDeltas array is
            // always one less than the number of literals (delta difference for n
            // elements is n-1). We have already written one element, write the
            // remaining numLiterals - 2 elements here
            writeInts(adjDeltas, 1, numLiterals - 2, fb);
        }
    }

    /**
     * Bitpack and write the input values to underlying output stream
     */
    private void writeInts(long[] input, int offset, int len, int bitSize)
            throws IOException
    {
        if (input == null || input.length < 1 || offset < 0 || len < 1 || bitSize < 1)
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

        // if bit size not match any of above ones
        int bitsLeft = 8;
        byte current = 0;
        for (int i = offset; i < (offset + len); i++)
        {
            long value = input[i];
            int bitsToWrite = bitSize;
            while (bitsToWrite > bitsLeft)
            {
                // add the bits to the bottom of the current word
                current |= value >>> (bitsToWrite - bitsLeft);
                // subtract out the bits we just added
                bitsToWrite -= bitsLeft;
                // zero out the bits above bitsToWrite
                value &= (1L << bitsToWrite) - 1;
                outputStream.write(current);
                current = 0;
                bitsLeft = 8;
            }
            bitsLeft -= bitsToWrite;
            current |= value << bitsLeft;
            if (bitsLeft == 0)
            {
                outputStream.write(current);
                current = 0;
                bitsLeft = 8;
            }
        }

        // flush
        if (bitsLeft != 8)
        {
            outputStream.write(current);
            current = 0;
            bitsLeft = 8;
        }
    }

    private void clear()
    {
        numLiterals = 0;
        encodingType = null;
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
        gapVsPatchList = null;
        min = 0;
        isFixedDelta = true;
    }

    private void initializeLiterals(long val)
    {
        literals[numLiterals++] = val;
        fixedRunLength = 1;
        variableRunLength = 1;
    }

    private void computeZigZagLiterals()
    {
        long zzEncVal = 0;
        for (int i = 0; i < numLiterals; i++)
        {
            if (isSigned)
            {
                zzEncVal = zigzagEncode(literals[i]);
            }
            else
            {
                zzEncVal = literals[i];
            }
            zigzagLiterals[i] = zzEncVal;
        }
    }

    private long zigzagEncode(long val)
    {
        return (val << 1) ^ (val >> 63);
    }

    private int getOpcode()
    {
        return encodingType.ordinal() << 6;
    }

    /**
     * Compute the number of bits required to represent pth percentile value
     */
    private int percentileBits(long[] data, int offset, int length, double p)
    {
        if ((p > 1.0) || (p <= 0.0))
        {
            return -1;
        }

        // histogram that stores the encoded bit requirement for each values.
        // maximum number of bits that can be encoded is 32
        int[] hist = new int[32];
        // compute histogram
        for (int i = offset; i < (offset + length); i++)
        {
            int idx = encodingUtils.encodeBitWidth(findClosestNumBits(data[i]));
            hist[idx] += 1;
        }

        int perLen = (int) (length * (1.0 - p));

        for (int i = hist.length - 1; i >= 0; i--)
        {
            perLen -= hist[i];
            if (perLen < 0)
            {
                return encodingUtils.decodeBitWidth(i);
            }
        }

        return 0;
    }

    /**
     * Count the number of bits required to encode the given value
     */
    private int findClosestNumBits(long value)
    {
        int count = 0;
        while (value != 0)
        {
            count++;
            value = value >>> 1;
        }
        return encodingUtils.getClosestFixedBits(count);
    }

    private int getClosestAlignedFixedBits(int n)
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

    private boolean isSafeSubtract(long left, long right)
    {
        return (left ^ right) >= 0 | (left ^ (left - right)) >= 0;
    }

    private void writeVulong(OutputStream output,
                             long value)
            throws IOException
    {
        while (true)
        {
            if ((value & ~0x7f) == 0)
            {
                output.write((byte) value);
                return;
            }
            else
            {
                output.write((byte) (0x80 | (value & 0x7f)));
                value >>>= 7;
            }
        }
    }

    private void writeVslong(OutputStream output,
                             long value)
            throws IOException
    {
        writeVulong(output, (value << 1) ^ (value >> 63));
    }
}
