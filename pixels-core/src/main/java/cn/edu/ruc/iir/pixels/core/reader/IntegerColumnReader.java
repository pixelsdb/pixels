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
    public IntegerColumnReader(TypeDescription type)
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
        ByteBuf inputBuffer = Unpooled.copiedBuffer(input);
        ByteBufInputStream inputStream = new ByteBufInputStream(inputBuffer);
        // if run length encoded
        if (encoding.getKind().equals(PixelsProto.ColumnEncoding.Kind.RUNLENGTH)) {
            RunLenIntDecoder decoder = new RunLenIntDecoder(inputStream, true);
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
        inputStream.close();
        inputBuffer.release();
    }
}
