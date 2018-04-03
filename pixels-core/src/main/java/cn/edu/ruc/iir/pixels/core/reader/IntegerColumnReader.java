package cn.edu.ruc.iir.pixels.core.reader;

import cn.edu.ruc.iir.pixels.core.PixelsProto;
import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.encoding.RunLenIntDecoder;
import cn.edu.ruc.iir.pixels.core.vector.ColumnVector;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;

import java.io.IOException;

import static com.google.common.base.Verify.verify;

/**
 * pixels
 *
 * @author guodong
 */
public class IntegerColumnReader
        extends ColumnReader
{
    private RunLenIntDecoder decoder;
    private ByteBuf inputBuffer;
    private ByteBufInputStream inputStream;

    IntegerColumnReader(TypeDescription type)
    {
        super(type);
    }

    /**
     * Read input buffer.
     *
     * @param input    input buffer
     * @param encoding encoding type
     * @param size     number of values to read
     * @param vector   vector to read into
     */
    @Override
    public void read(byte[] input, PixelsProto.ColumnEncoding encoding,
                     int offset, int size, int pixelStride, ColumnVector vector) throws IOException
    {
        // if read from start, init the stream and decoder
        if (offset == 0) {
            if (inputStream != null) {
                inputStream.close();
            }
            if (inputBuffer != null) {
                inputBuffer.release();
            }
            inputBuffer = Unpooled.copiedBuffer(input);
            inputStream = new ByteBufInputStream(inputBuffer);
            decoder = new RunLenIntDecoder(inputStream, true);
        }
        // if run length encoded
        if (encoding.getKind().equals(PixelsProto.ColumnEncoding.Kind.RUNLENGTH)) {
            ByteBufAllocator allocator = PooledByteBufAllocator.DEFAULT;
            ByteBuf buf = allocator.buffer();
            int readIdx = 0;
            while (decoder.hasNext() && readIdx < size) {
                vector.add(decoder.next());
                readIdx++;
            }
            verify(!buf.isReadable());
            buf.release();
        }
        // if not encoded
        else {
            byte firstByte = inputStream.readByte();
            boolean isLong = firstByte == (byte) 1;
            // if long
            if (isLong) {
                for (int i = 0; i < size; i++) {
                    vector.add(inputStream.readLong());
                }
            }
            // if int
            else {
                for (int i = 0; i < size; i++) {
                    vector.add(inputStream.readInt());
                }
            }
            verify(inputStream.available() == 0);
        }
    }
}
