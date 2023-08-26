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
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.core.reader;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;
import io.pixelsdb.pixels.core.PixelsProto;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.encoding.RunLenIntDecoder;
import io.pixelsdb.pixels.core.exception.PixelsReaderException;
import io.pixelsdb.pixels.core.utils.BitUtils;
import io.pixelsdb.pixels.core.utils.DynamicIntArray;
import io.pixelsdb.pixels.core.vector.BinaryColumnVector;
import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.pixelsdb.pixels.core.vector.DictionaryColumnVector;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author guodong, hank
 * @create 2018-01-22
 * @update 2023-08-21 support nulls padding
 */
public class StringColumnReader extends ColumnReader
{
    private int dictContentOffset;

    private int dictStartsOffset;

    private ByteBuf inputBuffer = null;
    /**
     * The string content in dictionary. It is backed by an on-heap array.
     * If inputBuffer is direct (has no backing array), bytes are copied from
     * inputBuffer into dictContentBuf.
     */
    private ByteBuf dictContentBuf = null;
    /**
     * elements' ABSOLUTE start offsets in dictContentBuf.
     */
    private int[] dictStarts = null;

    /**
     * The encoded dictionary id (if dictionary encoded) or
     * the origin string content (if not dictionary encoded).
     * In case of not dictionary encoded, we ensure it is backed by an on-heap array.
     * If inputBuffer is direct (has no backing array), bytes are copied from
     * inputBuffer into dictContentBuf.
     */
    private ByteBuf contentBuf = null;
    /**
     * The start offsets of the elements in the content if not dictionary encoded.
     * The first start offset is 0, and the last start offset of the length of the content.
     */
    private ByteBuf startsBuf = null;
    /**
     * The start offset of the current element in the content if not dictionary encoded.
     */
    private int currentStart = 0;
    /**
     * The next element in the content if not dictionary encoded.
     */
    private int nextStart = 0;
    /**
     * RLE decoder of dictionary encoded ids if dictionary encoded.
     */
    private RunLenIntDecoder contentDecoder = null;
    /**
     * Offset of isNull array (bit-packed) in the column chunk.
     */
    private int isNullOffset = 0;

    /**
     * Offset of content array when encode is not enabled.
     */
    private int bufferOffset = 0;
    /**
     * This is the predicted size (number of elements) of dictionary.
     */
    private static final int DEFAULT_STARTS_SIZE = 1024;

    StringColumnReader(TypeDescription type)
    {
        super(type);
    }

    /**
     * Closes this column reader and releases any resources associated
     * with it. If the column reader is already closed then invoking this
     * method has no effect.
     * <p>
     * <p> As noted in {@link AutoCloseable#close()}, cases where the
     * close may fail require careful attention. It is strongly advised
     * to relinquish the underlying resources and to internally
     * <em>mark</em> the {@code Closeable} as closed, prior to throwing
     * the {@code IOException}.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException
    {
        this.inputBuffer = null;
        this.contentBuf = null;
        this.startsBuf = null;
        this.currentStart = 0;
        this.nextStart = 0;
        this.dictContentBuf = null;
        this.dictStarts = null;
        if (this.contentDecoder != null)
        {
            this.contentDecoder.close();
            this.contentDecoder = null;
        }
    }

    /**
     * Read values from input buffer.
     *
     * @param input    input buffer
     * @param encoding encoding type
     * @param offset   starting reading offset of values
     * @param size     number of values to read
     * @param pixelStride the stride (number of rows) in a pixels.
     * @param vectorIndex the index from where we start reading values into the vector
     * @param vector   vector to read values into
     * @param chunkIndex the metadata of the column chunk to read.
     * @throws IOException
     */
    @Override
    public void read(ByteBuffer input, PixelsProto.ColumnEncoding encoding,
                     int offset, int size, int pixelStride, final int vectorIndex,
                     ColumnVector vector, PixelsProto.ColumnChunkIndex chunkIndex)
            throws IOException
    {
        if (offset == 0)
        {
            if (inputBuffer != null)
            {
                inputBuffer.release();
            }
            // no memory copy
            boolean littleEndian = chunkIndex.hasLittleEndian() && chunkIndex.getLittleEndian();
            ByteOrder byteOrder = littleEndian ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
            inputBuffer = Unpooled.wrappedBuffer(input).order(byteOrder);
            readContent(input.remaining(), encoding, byteOrder);
            isNullOffset = chunkIndex.getIsNullOffset();
            bufferOffset = 0;
            hasNull = true;
            elementIndex = 0;
        }
        boolean nullsPadding = chunkIndex.hasNullsPadding() && chunkIndex.getNullsPadding();
        // if dictionary encoded
        if (encoding.getKind().equals(PixelsProto.ColumnEncoding.Kind.DICTIONARY))
        {
            boolean cascadeRLE = false;
            if (encoding.hasCascadeEncoding() && encoding.getCascadeEncoding().getKind()
                    .equals(PixelsProto.ColumnEncoding.Kind.RUNLENGTH))
            {
                cascadeRLE = true;
            }
            if (vector instanceof BinaryColumnVector)
            {
                BinaryColumnVector columnVector = (BinaryColumnVector) vector;
                // read original bytes
                // we get bytes here to reduce memory copies and avoid creating many small byte arrays
                byte[] buffer = dictContentBuf.array();
                int numLeft = size, numToRead, bytesToDeCompact;
                for (int i = vectorIndex; numLeft > 0;)
                {
                    if (elementIndex / pixelStride < (elementIndex + numLeft) / pixelStride)
                    {
                        // read to the end of the current pixel
                        numToRead = pixelStride - elementIndex % pixelStride;
                    } else
                    {
                        numToRead = numLeft;
                    }
                    bytesToDeCompact = (numToRead + 7) / 8;
                    // read isNull
                    int pixelId = elementIndex / pixelStride;
                    hasNull = chunkIndex.getPixelStatistics(pixelId).getStatistic().getHasNull();
                    if (hasNull)
                    {
                        BitUtils.bitWiseDeCompactBE(columnVector.isNull, i, numToRead, inputBuffer, isNullOffset);
                        isNullOffset += bytesToDeCompact;
                        columnVector.noNulls = false;
                    } else
                    {
                        Arrays.fill(columnVector.isNull, i, i + numToRead, false);
                    }
                    for (int j = i; j < i + numToRead; ++j)
                    {
                        if (hasNull && columnVector.isNull[j])
                        {
                            if ((!cascadeRLE) && nullsPadding)
                            {
                                contentBuf.skipBytes(Integer.BYTES);
                            }
                        }
                        else
                        {
                            int originId = cascadeRLE ? (int) contentDecoder.next() : contentBuf.readInt();
                            int tmpLen = dictStarts[originId + 1] - dictStarts[originId];
                            // use setRef instead of setVal to reduce memory copy.
                            columnVector.setRef(j, buffer, dictStarts[originId], tmpLen);
                        }
                    }
                    // update variables
                    numLeft -= numToRead;
                    elementIndex += numToRead;
                    i += numToRead;
                }
            }
            else if (vector instanceof DictionaryColumnVector)
            {
                DictionaryColumnVector columnVector = (DictionaryColumnVector) vector;
                if (columnVector.dictArray == null)
                {
                    columnVector.dictArray = dictContentBuf.array();
                    columnVector.dictOffsets = dictStarts;
                }
                checkArgument(columnVector.dictArray == dictContentBuf.array(),
                        "dictionaries in the column vector and the input buffer are inconsistent");

                int numLeft = size, numToRead, bytesToDeCompact;
                for (int i = vectorIndex; numLeft > 0;)
                {
                    if (elementIndex / pixelStride < (elementIndex + numLeft) / pixelStride)
                    {
                        // read to the end of the current pixel
                        numToRead = pixelStride - elementIndex % pixelStride;
                    } else
                    {
                        numToRead = numLeft;
                    }
                    bytesToDeCompact = (numToRead + 7) / 8;
                    // read isNull
                    int pixelId = elementIndex / pixelStride;
                    hasNull = chunkIndex.getPixelStatistics(pixelId).getStatistic().getHasNull();
                    if (hasNull)
                    {
                        BitUtils.bitWiseDeCompactBE(columnVector.isNull, i, numToRead, inputBuffer, isNullOffset);
                        isNullOffset += bytesToDeCompact;
                        columnVector.noNulls = false;
                    } else
                    {
                        Arrays.fill(columnVector.isNull, i, i + numToRead, false);
                    }
                    for (int j = i; j < i + numToRead; ++j)
                    {
                        if (hasNull && columnVector.isNull[j])
                        {
                            if ((!cascadeRLE) && nullsPadding)
                            {
                                contentBuf.skipBytes(Integer.BYTES);
                            }
                        }
                        else
                        {
                            int originId = cascadeRLE ? (int) contentDecoder.next() : contentBuf.readInt();
                            columnVector.setId(i + vectorIndex, originId);
                        }
                    }
                    // update variables
                    numLeft -= numToRead;
                    elementIndex += numToRead;
                    i += numToRead;
                }
            }
            else
            {
                throw new IllegalArgumentException("unsupported column vector type: " + vector.getClass().getName());
            }
        }
        // if un-encoded
        else
        {
            BinaryColumnVector columnVector = (BinaryColumnVector) vector;
            // read values
            // we get bytes here to reduce memory copies and avoid creating many small byte arrays.
            byte[] buffer = contentBuf.array();
            int numLeft = size, numToRead, bytesToDeCompact;
            for (int i = vectorIndex; numLeft > 0;)
            {
                if (elementIndex / pixelStride < (elementIndex + numLeft) / pixelStride)
                {
                    // read to the end of the current pixel
                    numToRead = pixelStride - elementIndex % pixelStride;
                } else
                {
                    numToRead = numLeft;
                }
                bytesToDeCompact = (numToRead + 7) / 8;
                // read isNull
                int pixelId = elementIndex / pixelStride;
                hasNull = chunkIndex.getPixelStatistics(pixelId).getStatistic().getHasNull();
                if (hasNull)
                {
                    BitUtils.bitWiseDeCompactBE(columnVector.isNull, i, numToRead, inputBuffer, isNullOffset);
                    isNullOffset += bytesToDeCompact;
                    columnVector.noNulls = false;
                } else
                {
                    Arrays.fill(columnVector.isNull, i, i + numToRead, false);
                }
                for (int j = i; j < i + numToRead; ++j)
                {
                    if (hasNull && columnVector.isNull[j])
                    {
                        if (nullsPadding)
                        {
                            currentStart = nextStart;
                            nextStart = startsBuf.readInt();
                            if (currentStart != nextStart)
                            {
                                throw new PixelsReaderException("corrupt start offsets detected while nulls padding is enabled");
                            }
                        }
                    }
                    else
                    {
                        currentStart = nextStart;
                        nextStart = startsBuf.readInt();
                        int len = nextStart - currentStart;
                        // use setRef instead of setVal to reduce memory copy
                        columnVector.setRef(j, buffer, bufferOffset, len);
                        bufferOffset += len;
                    }
                }
                // update variables
                numLeft -= numToRead;
                elementIndex += numToRead;
                i += numToRead;
            }
        }
    }

    /**
     * In this method, we have reduced most of significant memory copies.
     */
    private void readContent(int inputLength, PixelsProto.ColumnEncoding encoding, ByteOrder byteOrder) throws IOException
    {
        if (encoding.getKind().equals(PixelsProto.ColumnEncoding.Kind.DICTIONARY))
        {
            // read offsets
            inputBuffer.markReaderIndex();
            inputBuffer.skipBytes(inputLength - 2 * Integer.BYTES);
            dictContentOffset = inputBuffer.readInt();
            dictStartsOffset = inputBuffer.readInt();
            inputBuffer.resetReaderIndex();
            // read buffers
            contentBuf = inputBuffer.slice(0, dictContentOffset).order(byteOrder);
            if (this.inputBuffer.hasArray())
            {
                dictContentBuf = inputBuffer.slice(dictContentOffset, dictStartsOffset - dictContentOffset);
            }
            else
            {
                /*
                 * Issue #374:
                 * If inputBuffer is read from pixels-cache or LocalFS (using direct read),
                 * in this case, it would be direct and is not backed by an array.
                 * In Pixels, string types (i.e., char, varchar, string) are mapped to byte[] internally for
                 * filtering, join, and aggregation.
                 * we currently do not support ByteBuffer for these types, hence we have to copy the dictionary
                 * int the direct inputBuffer into a byte array.
                 * TODO: support ByteBuffer for string types.
                 */
                byte[] bytes = new byte[dictStartsOffset - dictContentOffset];
                inputBuffer.getBytes(dictContentOffset, bytes, 0, dictStartsOffset - dictContentOffset);
                dictContentBuf = Unpooled.wrappedBuffer(bytes);
            }

            // read starts, the last two integers (8 bytes) are the origin offset and starts offset
            int startsBufLength = inputLength - dictStartsOffset - 2 * Integer.BYTES;
            ByteBuf startsBuf = inputBuffer.slice(dictStartsOffset, startsBufLength).order(byteOrder);

            /*
             * DO NOT use dictContentOffset as bufferStart, as multiple input buffers read from disk (not from pixels cache)
             * may share the same backing array, each starting from different offsets. dictContentOffset equals to
             * dictContentBuf.arrayOffset() only when the input buffer starts from the first byte of backing array.
             */
            int bufferStart = dictContentBuf.arrayOffset();

            if (encoding.hasCascadeEncoding() && encoding.getCascadeEncoding().getKind()
                    .equals(PixelsProto.ColumnEncoding.Kind.RUNLENGTH))
            {
                RunLenIntDecoder startsDecoder = new RunLenIntDecoder(new ByteBufInputStream(startsBuf), false);
                /*
                 * Issue #124:
                 * Try to avoid using dynamic array if dictionary size is known, so that to reduce GC.
                 */
                if (encoding.hasDictionarySize())
                {
                    dictStarts = new int[encoding.getDictionarySize() + 1];
                    int i = 0;
                    while (startsDecoder.hasNext())
                    {
                        dictStarts[i++] = bufferStart + (int) startsDecoder.next();
                    }
                }
                else
                {
                    DynamicIntArray startsArray;
                    startsArray = new DynamicIntArray(DEFAULT_STARTS_SIZE);
                    while (startsDecoder.hasNext())
                    {
                        startsArray.add(bufferStart + (int) startsDecoder.next());
                    }
                    dictStarts = startsArray.toArray();
                }

                /*
                 * Issue #498:
                 * We no longer read the orders array (encoded-id to key-index mapping) from files.
                 * Encoded id is exactly the index of the key in the dictionary.
                 */

                contentDecoder = new RunLenIntDecoder(new ByteBufInputStream(contentBuf), false);
            }
            else
            {
                if (startsBufLength % Integer.BYTES != 0)
                {
                    throw new PixelsReaderException("the length of the starts array buffer is invalid");
                }
                int startsSize = startsBufLength / Integer.BYTES;
                if (encoding.hasDictionarySize() && encoding.getDictionarySize() + 1 != startsSize)
                {
                    throw new PixelsReaderException("the dictionary size is inconsistent with the size of the starts array");
                }
                dictStarts = new int[startsSize];
                for (int i = 0; i < startsSize; ++i)
                {
                    dictStarts[i] = bufferStart + startsBuf.readInt();
                }
                contentDecoder = null;
            }
        }
        else
        {
            // read lens field offset
            inputBuffer.markReaderIndex();
            inputBuffer.skipBytes(inputLength - Integer.BYTES);
            int startsOffset = inputBuffer.readInt();
            inputBuffer.resetReaderIndex();
            // read strings
            if (this.inputBuffer.isDirect())
            {
                byte[] bytes = new byte[startsOffset];
                inputBuffer.getBytes(0, bytes, 0, startsOffset);
                contentBuf = Unpooled.wrappedBuffer(bytes);
            }
            else
            {
                contentBuf = inputBuffer.slice(0, startsOffset);
                bufferOffset = contentBuf.arrayOffset();
            }
            /*
             * Issue #539:
             * Read the starts field.
             * In the current version (4.1.77-Final) of netty, ByteBuf.slice() extends the byte order from inputBuffer.
             * However, for safety, we still explicitly set the byte order for startsBuf.
             */
            startsBuf = inputBuffer.slice(startsOffset, inputLength - Integer.BYTES - startsOffset).order(byteOrder);
            nextStart = startsBuf.readInt(); // read out the first start offset, which is 0
        }
    }
}
