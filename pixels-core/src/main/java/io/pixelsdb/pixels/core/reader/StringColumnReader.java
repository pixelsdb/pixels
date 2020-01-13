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

import io.pixelsdb.pixels.core.PixelsProto;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.encoding.RunLenIntDecoder;
import io.pixelsdb.pixels.core.utils.BitUtils;
import io.pixelsdb.pixels.core.utils.DynamicIntArray;
import io.pixelsdb.pixels.core.vector.BinaryColumnVector;
import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.nio.ByteBuffer;

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
     * Mapping encoded dictionary id to element index in origins.
     */
    private int[] orders = null;
    /**
     * elements' relative start offsets in origins.
     */
    private int[] starts = null;
    /**
     * Number of origin elements in dictionary.
     */
    private int originNum;

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
        this.orders = null;
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
     * @param size     number of values to read
     * @param vector   vector to read into
     * @throws IOException
     */
    @Override
    public void read(ByteBuffer input, PixelsProto.ColumnEncoding encoding,
                     int offset, int size, int pixelStride, final int vectorIndex,
                     ColumnVector vector, PixelsProto.ColumnChunkIndex chunkIndex)
            throws IOException
    {

        BinaryColumnVector columnVector = (BinaryColumnVector) vector;
        if (offset == 0)
        {
            if (inputBuffer != null)
            {
                inputBuffer.release();
            }
            // no memory copy
            inputBuffer = Unpooled.wrappedBuffer(input);
            readContent(input.limit(), encoding);
            isNullOffset = (int) chunkIndex.getIsNullOffset();
            hasNull = true;
            elementIndex = 0;
            isNullBitIndex = 8;
        }
        // if dictionary encoded
        if (encoding.getKind().equals(PixelsProto.ColumnEncoding.Kind.DICTIONARY))
        {
            // read original bytes
            // we get bytes here to reduce memory copies and avoid creating many small byte arrays.
            byte[] buffer = originsBuf.array();
            // The available first byte in buffer should start from originsOffset.
            // bufferStart is the first byte within buffer.
            // DO NOT use originsOffset as bufferStart, as multiple input byte buffer read
            // from disk (not from pixels cache) may share the same backing array, each of them starts
            // from a different offset. originsOffset equals to originsBuf.arrayOffset() only when a
            // input buffer starts from the first byte of backing array.
            int bufferStart = originsBuf.arrayOffset();
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
                }
                else
                {
                    int originId = orders[(int) contentDecoder.next()];
                    int tmpLen;
                    if (originId < originNum - 1)
                    {
                        tmpLen = starts[originId + 1] - starts[originId];
                    }
                    else
                    {
                        tmpLen = startsOffset - originsOffset - starts[originId];
                    }
                    // use setRef instead of setVal to reduce memory copy.
                    columnVector.setRef(i + vectorIndex, buffer, bufferStart + starts[originId], tmpLen);
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
            int bufferOffset = contentBuf.arrayOffset();
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
                }
                else
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

    private void readContent(int inputLength, PixelsProto.ColumnEncoding encoding)
            throws IOException
    {
        // TODO: reduce memory copy in this method.
        if (encoding.getKind().equals(PixelsProto.ColumnEncoding.Kind.DICTIONARY))
        {
            // read offsets
            inputBuffer.markReaderIndex();
            inputBuffer.skipBytes(inputLength - 3 * Integer.BYTES);
            originsOffset = inputBuffer.readInt();
            startsOffset = inputBuffer.readInt();
            int ordersOffset = inputBuffer.readInt();
            inputBuffer.resetReaderIndex();
            // read buffers
            contentBuf = inputBuffer.slice(0, originsOffset);
            if (this.inputBuffer.isDirect())
            {
                byte[] bytes = new byte[startsOffset - originsOffset];
                inputBuffer.getBytes(originsOffset, bytes, 0, startsOffset - originsOffset);
                originsBuf = Unpooled.wrappedBuffer(bytes);
            }
            else
            {
                originsBuf = inputBuffer.slice(originsOffset, startsOffset - originsOffset);
            }
            // read starts and orders
            ByteBuf startsBuf = inputBuffer.slice(startsOffset, ordersOffset - startsOffset);
            ByteBuf ordersBuf = inputBuffer.slice(ordersOffset, inputLength - ordersOffset);
            DynamicIntArray startsArray = new DynamicIntArray(1024);
            RunLenIntDecoder startsDecoder = new RunLenIntDecoder(new ByteBufInputStream(startsBuf), false);
            while (startsDecoder.hasNext())
            {
                startsArray.add((int) startsDecoder.next());
            }
            this.originNum = startsArray.size();
            RunLenIntDecoder ordersDecoder = new RunLenIntDecoder(new ByteBufInputStream(ordersBuf), false);
            starts = startsArray.toArray();
            orders = new int[originNum];
            for (int i = 0; i < originNum && ordersDecoder.hasNext(); i++)
            {
                orders[i] = (int) ordersDecoder.next();
            }
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
