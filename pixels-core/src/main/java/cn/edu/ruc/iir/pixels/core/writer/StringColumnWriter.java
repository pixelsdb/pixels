package cn.edu.ruc.iir.pixels.core.writer;

import cn.edu.ruc.iir.pixels.common.utils.Constants;
import cn.edu.ruc.iir.pixels.core.PixelsProto;
import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.encoding.RunLenIntEncoder;
import cn.edu.ruc.iir.pixels.core.utils.DynamicIntArray;
import cn.edu.ruc.iir.pixels.core.utils.StringRedBlackTree;
import cn.edu.ruc.iir.pixels.core.vector.BytesColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.ColumnVector;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * String column writer.
 * <p>
 * The string column chunk consists of seven fields:
 * 1. pixels field (run length encoded pixels after dictionary encoding or un-encoded string values)
 * 2. lengths field (run length encoded raw string length)
 * 3. origins field (distinct string bytes array)
 * 4. starts field (starting offsets indicating starting points of each string in the origins field)
 * 5. orders field (dumped orders array mapping dictionary encoded value to final sorted order)
 * 6. lengths field offset (an integer value indicating offset of the lengths field in the chunk)
 * 7. origins field offset (an integer value indicating offset of the origins field in the chunk)
 * 8. starts field offset (an integer value indicating offset of the starts field in the chunk)
 * 9. orders field offset (an integer value indicating offset of the orders field in the chunk)
 * <p>
 * Pixels field is necessary in all cases.
 * Lengths field only exists when un-encoded.
 * Other fields only exist when dictionary encoding is enabled.
 *
 * @author guodong
 */
public class StringColumnWriter extends BaseColumnWriter
{
    private final long[] curPixelVector = new long[pixelStride];      // current vector holding encoded values of string
    private final DynamicIntArray lensArray = new DynamicIntArray();  // lengths of each string when un-encoded
    private final StringRedBlackTree dictionary = new StringRedBlackTree(Constants.INIT_DICT_SIZE);
    private boolean futureUseDictionaryEncoding;
    private boolean currentUseDictionaryEncoding;
    private boolean doneDictionaryEncodingCheck = false;

    public StringColumnWriter(TypeDescription schema, int pixelStride, boolean isEncoding)
    {
        super(schema, pixelStride, isEncoding);
        this.futureUseDictionaryEncoding = isEncoding;
        this.currentUseDictionaryEncoding = isEncoding;
        encoder = new RunLenIntEncoder(false, true);
    }

    @Override
    public int write(ColumnVector vector, int size) throws IOException
    {
        currentUseDictionaryEncoding = futureUseDictionaryEncoding;
        BytesColumnVector columnVector = (BytesColumnVector) vector;
        byte[][] values = columnVector.vector;
        int[] vLens = columnVector.lens;
        int[] vOffsets = columnVector.start;
        int curPartLength;
        int curPartOffset = 0;
        int nextPartLength = size;

        if (currentUseDictionaryEncoding)
        {
            while ((curPixelIsNullIndex + nextPartLength) >= pixelStride)
            {
                curPartLength = pixelStride - curPixelIsNullIndex;
                writeCurPartWithDict(columnVector, values, vLens, vOffsets, curPartLength, curPartOffset);
                newPixel();
                curPartOffset += curPartLength;
                nextPartLength = size - curPartOffset;
            }

            curPartLength = nextPartLength;
            writeCurPartWithDict(columnVector, values, vLens, vOffsets, curPartLength, curPartOffset);
        }
        else
        {
            // directly add to outputStream if not using dictionary encoding
            while ((curPixelIsNullIndex + nextPartLength) >= pixelStride)
            {
                curPartLength = pixelStride - curPixelIsNullIndex;
                writeCurPartWithoutDict(columnVector, values, vLens, vOffsets, curPartLength, curPartOffset);
                newPixel();
                curPartOffset += curPartLength;
                nextPartLength = size - curPartOffset;
            }

            curPartLength = nextPartLength;
            writeCurPartWithoutDict(columnVector, values, vLens, vOffsets, curPartLength, curPartOffset);
        }
        return outputStream.size();
    }

    private void writeCurPartWithoutDict(BytesColumnVector columnVector, byte[][] values, int[] vLens, int[] vOffsets, int curPartLength, int curPartOffset)
    {
        for (int i = 0; i < curPartLength; i++)
        {
            curPixelEleIndex++;
            if (columnVector.isNull[i + curPartOffset])
            {
                hasNull = true;
                pixelStatRecorder.increment();
            }
            else
            {
                outputStream.write(values[curPartOffset + i], vOffsets[curPartOffset + i], vLens[curPartOffset + i]);
                lensArray.add(vLens[curPartOffset + i]);
                pixelStatRecorder
                        .updateString(values[curPartOffset + i], vOffsets[curPartOffset + i], vLens[curPartOffset + i],
                                      1);
            }
        }
        System.arraycopy(columnVector.isNull, curPartOffset, isNull, curPixelIsNullIndex, curPartLength);
        curPixelIsNullIndex += curPartLength;
    }

    private void writeCurPartWithDict(BytesColumnVector columnVector, byte[][] values, int[] vLens, int[] vOffsets, int curPartLength, int curPartOffset)
    {
        for (int i = 0; i < curPartLength; i++)
        {
            curPixelEleIndex++;
            if (columnVector.isNull[i + curPartOffset])
            {
                hasNull = true;
                pixelStatRecorder.increment();
            }
            else
            {
                curPixelVector[curPixelVectorIndex++] = dictionary
                        .add(values[curPartOffset + i], vOffsets[curPartOffset + i], vLens[curPartOffset + i]);
                pixelStatRecorder
                        .updateString(values[curPartOffset + i], vOffsets[curPartOffset + i], vLens[curPartOffset + i],
                                      1);
            }
        }
        System.arraycopy(columnVector.isNull, curPartOffset, isNull, curPixelIsNullIndex, curPartLength);
        curPixelIsNullIndex += curPartLength;
    }

    @Override
    public void newPixel() throws IOException
    {
        if (currentUseDictionaryEncoding)
        {
            // for dictionary encoding. run length encode again.
            outputStream.write(encoder.encode(curPixelVector, 0, curPixelVectorIndex));
        }
        // else ignore outputStream

        super.newPixel();
    }

    @Override
    public void flush() throws IOException
    {
        // flush out pixels field
        super.flush();
        // check if continue using dictionary encoding or not in the coming chunks
        if (!doneDictionaryEncodingCheck)
        {
            checkDictionaryEncoding();
        }
        // flush out other fields
        if (currentUseDictionaryEncoding)
        {
            flushDictionary();
        }
        else
        {
            flushLens();
        }
    }

    @Override
    public PixelsProto.ColumnEncoding.Builder getColumnChunkEncoding()
    {
        if (currentUseDictionaryEncoding)
        {
            return PixelsProto.ColumnEncoding.newBuilder()
                                             .setKind(PixelsProto.ColumnEncoding.Kind.DICTIONARY);
        }
        return PixelsProto.ColumnEncoding.newBuilder()
                                         .setKind(PixelsProto.ColumnEncoding.Kind.NONE);
    }

    @Override
    public void close() throws IOException
    {
        lensArray.clear();
        dictionary.clear();
        encoder.close();
        super.close();
    }

    private void flushLens() throws IOException
    {
        int lensFieldOffset = outputStream.size();
        long[] tmpLens = new long[lensArray.size()];
        for (int i = 0; i < lensArray.size(); i++)
        {
            tmpLens[i] = lensArray.get(i);
        }
        lensArray.clear();
        outputStream.write(encoder.encode(tmpLens));

        ByteBuffer offsetBuf = ByteBuffer.allocate(Integer.BYTES);
        offsetBuf.putInt(lensFieldOffset);
        outputStream.write(offsetBuf.array());
    }

    private void flushDictionary() throws IOException
    {
        int originsFieldOffset;
        int startsFieldOffset;
        int ordersFieldOffset;
        int size = dictionary.size();
        long[] starts = new long[size];
        long[] orders = new long[size];

        originsFieldOffset = outputStream.size();

        // recursively visit the red black tree, and fill origins field, get starts array and orders array
        dictionary.visit(new StringRedBlackTree.Visitor()
        {
            private int initStart = 0;
            private int currentId = 0;

            @Override
            public void visit(StringRedBlackTree.VisitorContext context) throws IOException
            {
                context.writeBytes(outputStream);
                starts[currentId] = initStart;
                initStart += context.getLength();
                orders[context.getOriginalPosition()] = currentId++;
            }
        });

        startsFieldOffset = outputStream.size();

        // write out run length starts array
        outputStream.write(encoder.encode(starts));
        ordersFieldOffset = outputStream.size();

        // write out run length orders array
        outputStream.write(encoder.encode(orders));

        ByteBuffer offsetsBuf = ByteBuffer.allocate(3 * Integer.BYTES);
        offsetsBuf.putInt(originsFieldOffset);
        offsetsBuf.putInt(startsFieldOffset);
        offsetsBuf.putInt(ordersFieldOffset);
        outputStream.write(offsetsBuf.array());
    }

    private void checkDictionaryEncoding()
    {
        int valueNum = outputStream.size() / Integer.BYTES;
        float ratio = valueNum > 0 ? (float) dictionary.size() / valueNum : 0.0f;
        futureUseDictionaryEncoding = ratio <= Constants.DICT_KEY_SIZE_THRESHOLD;
        doneDictionaryEncodingCheck = true;
    }
}
