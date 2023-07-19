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
import io.pixelsdb.pixels.core.utils.BitUtils;
import io.pixelsdb.pixels.core.utils.DynamicIntArray;
import io.pixelsdb.pixels.core.vector.BinaryColumnVector;
import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.pixelsdb.pixels.core.vector.DictionaryColumnVector;

import java.io.IOException;
import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author guodong
 * @author hank
 */
public class StringColumnReader
        extends ColumnReader
{
    private int originsOffset;

    private int startsOffset;

    private ByteBuf inputBuffer = null;
    /**
     * The origin string content in dictionary.
     * It is copied into heap from inputBuffer and has a backing array.
     */
    private ByteBuf originsBuf = null;
    /**
     * elements' ABSOLUTE start offsets in originsBuf.
     */
    private int[] starts = null;

    private byte[] isNull = new byte[8];
    /**
     * The encoded dictionary id (if dictionary encoded) or
     * the origin string content (if not dictionary encoded).
     * In case of not dictionary encoded, it is copied into heap from inputBuffer
     * and has a backing array.
     */
    private ByteBuf contentBuf = null;
    /**
     * RLE decoder of string content element length if no dictionary encoded.
     */
    private RunLenIntDecoder lensDecoder = null;
    /**
     * RLE decoder of dictionary encoded ids if dictionary encoded.
     */
    private RunLenIntDecoder contentDecoder = null;
    /**
     * Offset of isNull array (bit-packed) in the column chunk.
     */
    private int isNullOffset = 0;

    private int isNullBitIndex = 0;

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
        this.contentBuf = null;
        this.inputBuffer = null;
        this.originsBuf = null;
        this.starts = null;
        this.isNull = null;
        if (this.contentDecoder != null)
        {
            this.contentDecoder.close();
            this.contentDecoder = null;
        }
        if (this.lensDecoder != null)
        {
            this.lensDecoder.close();
            this.lensDecoder = null;
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
            inputBuffer = Unpooled.wrappedBuffer(input);
            readContent(input.remaining(), encoding);
            isNullOffset = (int) chunkIndex.getIsNullOffset();
            bufferOffset = 0;
            hasNull = true;
            elementIndex = 0;
            isNullBitIndex = 8;
        }
        if (vector instanceof BinaryColumnVector)
        {
            BinaryColumnVector columnVector = (BinaryColumnVector) vector;
            // if dictionary encoded
            if (encoding.getKind().equals(PixelsProto.ColumnEncoding.Kind.DICTIONARY))
            {
                // read original bytes
                // we get bytes here to reduce memory copies and avoid creating many small byte arrays.
                byte[] buffer = originsBuf.array();
                for (int i = 0; i < size; i++)
                {
                    if (elementIndex % pixelStride == 0)
                    {
                        int pixelId = elementIndex / pixelStride;
                        hasNull = chunkIndex.getPixelStatistics(pixelId).getStatistic().getHasNull();
                        if (hasNull && isNullBitIndex > 0)
                        {
                            BitUtils.bitWiseDeCompact(isNull, inputBuffer, isNullOffset++, 1);
                            isNullBitIndex = 0;
                        }
                    }
                    if (hasNull && isNullBitIndex >= 8)
                    {
                        BitUtils.bitWiseDeCompact(isNull, inputBuffer, isNullOffset++, 1);
                        isNullBitIndex = 0;
                    }
                    if (hasNull && isNull[isNullBitIndex] == 1)
                    {
                        columnVector.isNull[i + vectorIndex] = true;
                        columnVector.noNulls = false;
                    } else
                    {
                        int originId = (int) contentDecoder.next();
                        int tmpLen = starts[originId + 1] - starts[originId];
                        // use setRef instead of setVal to reduce memory copy.
                        columnVector.setRef(i + vectorIndex, buffer, starts[originId], tmpLen);
                    }
                    if (hasNull)
                    {
                        isNullBitIndex++;
                    }
                    elementIndex++;
                }
            }
            // if un-encoded
            else
            {
                // read values
                // we get bytes here to reduce memory copies and avoid creating many small byte arrays.
                byte[] buffer = contentBuf.array();
                for (int i = 0; i < size; i++)
                {
                    if (elementIndex % pixelStride == 0)
                    {
                        int pixelId = elementIndex / pixelStride;
                        hasNull = chunkIndex.getPixelStatistics(pixelId).getStatistic().getHasNull();
                        if (hasNull && isNullBitIndex > 0)
                        {
                            BitUtils.bitWiseDeCompact(isNull, inputBuffer, isNullOffset++, 1);
                            isNullBitIndex = 0;
                        }
                    }
                    if (hasNull && isNullBitIndex >= 8)
                    {
                        BitUtils.bitWiseDeCompact(isNull, inputBuffer, isNullOffset++, 1);
                        isNullBitIndex = 0;
                    }
                    if (hasNull && isNull[isNullBitIndex] == 1)
                    {
                        columnVector.isNull[i + vectorIndex] = true;
                        columnVector.noNulls = false;
                    } else
                    {
                        int len = (int) lensDecoder.next();
                        // use setRef instead of setVal to reduce memory copy.
                        columnVector.setRef(i + vectorIndex, buffer, bufferOffset, len);
                        bufferOffset += len;
                    }
                    if (hasNull)
                    {
                        isNullBitIndex++;
                    }
                    elementIndex++;
                }
            }
        }
        else if (vector instanceof DictionaryColumnVector)
        {
            DictionaryColumnVector columnVector = (DictionaryColumnVector) vector;
            if (columnVector.dictArray == null)
            {
                columnVector.dictArray = originsBuf.array();
                columnVector.dictOffsets = starts;
            }
            checkArgument(columnVector.dictArray == originsBuf.array(),
                    "dictionaries from the column vector and the origins buffer are not consistent");

            for (int i = 0; i < size; i++)
            {
                if (elementIndex % pixelStride == 0)
                {
                    int pixelId = elementIndex / pixelStride;
                    hasNull = chunkIndex.getPixelStatistics(pixelId).getStatistic().getHasNull();
                    if (hasNull && isNullBitIndex > 0)
                    {
                        BitUtils.bitWiseDeCompact(isNull, inputBuffer, isNullOffset++, 1);
                        isNullBitIndex = 0;
                    }
                }
                if (hasNull && isNullBitIndex >= 8)
                {
                    BitUtils.bitWiseDeCompact(isNull, inputBuffer, isNullOffset++, 1);
                    isNullBitIndex = 0;
                }
                if (hasNull && isNull[isNullBitIndex] == 1)
                {
                    columnVector.isNull[i + vectorIndex] = true;
                    columnVector.noNulls = false;
                } else
                {
                    int originId = (int) contentDecoder.next();
                    columnVector.setId(i + vectorIndex, originId);
                }
                if (hasNull)
                {
                    isNullBitIndex++;
                }
                elementIndex++;
            }
        }
        else
        {
            throw new IllegalArgumentException("unexpected column vector type: " + vector.getClass().getName());
        }
    }

    /**
     * In this method, we have reduced most of significant memory copies.
     */
    private void readContent(int inputLength, PixelsProto.ColumnEncoding encoding)
            throws IOException
    {
        if (encoding.getKind().equals(PixelsProto.ColumnEncoding.Kind.DICTIONARY))
        {
            // read offsets
            inputBuffer.markReaderIndex();
            inputBuffer.skipBytes(inputLength - 3 * Integer.BYTES);
            originsOffset = inputBuffer.readInt();
            startsOffset = inputBuffer.readInt();
            inputBuffer.resetReaderIndex();
            // read buffers
            contentBuf = inputBuffer.slice(0, originsOffset);
            if (this.inputBuffer.hasArray())
            {
                originsBuf = inputBuffer.slice(originsOffset, startsOffset - originsOffset);
            }
            else
            {
                /**
                 * Issue #374:
                 * If inputBuffer is read from pixels-cache or LocalFS (using direct read),
                 * in this case, it would be direct and is not backed by an array.
                 * In Pixels, string types (i.e., char, varchar, string) are mapped to byte[] internally for
                 * filtering, join, and aggregation.
                 * we currently do not support ByteBuffer for these types, hence we have to copy the dictionary
                 * int the direct inputBuffer into a byte array.
                 * TODO: support ByteBuffer for string types.
                 */
                byte[] bytes = new byte[startsOffset - originsOffset];
                inputBuffer.getBytes(originsOffset, bytes, 0, startsOffset - originsOffset);
                originsBuf = Unpooled.wrappedBuffer(bytes);
            }
            // read starts
            ByteBuf startsBuf = inputBuffer.slice(startsOffset, inputLength - startsOffset);

            // DO NOT use originsOffset as bufferStart, as multiple input buffers read
            // from disk (not from pixels cache) may share the same backing array, each starting
            // from different offsets. originsOffset equals to originsBuf.arrayOffset() only when the
            // input buffer starts from the first byte of backing array.
            int bufferStart = originsBuf.arrayOffset();
            RunLenIntDecoder startsDecoder = new RunLenIntDecoder(new ByteBufInputStream(startsBuf), false);
            /**
             * Issue #124:
             * Try to avoid using dynamic array if dictionary size is known, so that to reduce GC.
             */
            if (encoding.hasDictionarySize())
            {
                starts = new int[encoding.getDictionarySize() + 1];
                int i = 0;
                while (startsDecoder.hasNext())
                {
                    starts[i++] = bufferStart + (int) startsDecoder.next();
                }
                starts[i] = bufferStart + startsOffset - originsOffset;
            }
            else
            {
                DynamicIntArray startsArray;
                startsArray = new DynamicIntArray(DEFAULT_STARTS_SIZE);
                while (startsDecoder.hasNext())
                {
                    startsArray.add(bufferStart + (int) startsDecoder.next());
                }
                startsArray.add(bufferStart + startsOffset - originsOffset);
                starts = startsArray.toArray();
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
            // read lens field offset
            inputBuffer.markReaderIndex();
            inputBuffer.skipBytes(inputLength - Integer.BYTES);
            int lensOffset = inputBuffer.readInt();
            inputBuffer.resetReaderIndex();
            // read strings
            if (this.inputBuffer.isDirect())
            {
                byte[] bytes = new byte[lensOffset];
                inputBuffer.getBytes(0, bytes, 0, lensOffset);
                contentBuf = Unpooled.wrappedBuffer(bytes);
            }
            else
            {
                contentBuf = inputBuffer.slice(0, lensOffset);
            }
            // read lens field
            ByteBuf lensBuf = inputBuffer.slice(lensOffset, inputLength - Integer.BYTES - lensOffset);
            lensDecoder = new RunLenIntDecoder(new ByteBufInputStream(lensBuf), false);
        }
    }
}
