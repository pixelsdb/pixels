package cn.edu.ruc.iir.pixels.core.reader;

import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.encoding.RunLenIntDecoder;
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
     * read int values
     * @param input input bytes
     * @return values (maybe should return ByteBuf or OutputStream?)
     * @throws IOException io exception thrown by <code>RunLenIntDecoder</code>
     * */
    @Override
    public long[] readInts(byte[] input, boolean isEncoding, int num)
            throws IOException
    {
        ByteBuf inputBuffer = Unpooled.copiedBuffer(input);
        ByteBufInputStream inputStream = new ByteBufInputStream(inputBuffer);
        long[] values = new long[num];
        // if run length encoded
        if (isEncoding) {
            RunLenIntDecoder decoder = new RunLenIntDecoder(inputStream, true);
            ByteBufAllocator allocator = PooledByteBufAllocator.DEFAULT;
            ByteBuf buf = allocator.buffer();
            while (decoder.hasNext()) {
                buf.writeLong(decoder.next());
                num++;
            }
            for (int i = 0; i < num; i++) {
                values[i] = buf.getLong(i);
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
                for (int i = 0; i < num; i++) {
                    values[i] = inputStream.readLong();
                }
            }
            // if int
            else {
                for (int i = 0; i < num; i++) {
                    values[i] = inputStream.readInt();
                }
            }
            verify(inputStream.available() == 0);
        }
        inputStream.close();
        inputBuffer.release();
        return values;
    }
}
