package cn.edu.ruc.iir.pixels.core.reader;

import cn.edu.ruc.iir.pixels.core.PixelsProto;
import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.encoding.RunLenIntDecoder;
import cn.edu.ruc.iir.pixels.core.vector.ColumnVector;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.sql.Timestamp;

/**
 * pixels timestamp column reader
 * All timestamp values are translated to the specified time zone after read from file.
 *
 * @author guodong
 */
public class TimestampColumnReader
        extends ColumnReader
{
    public TimestampColumnReader(TypeDescription type)
    {
        super(type);
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
    public void read(byte[] input, PixelsProto.ColumnEncoding encoding,
                     int offset, int size, ColumnVector vector) throws IOException
    {
        ByteBuf inputBuffer = Unpooled.copiedBuffer(input);
        ByteBufInputStream inputStream = new ByteBufInputStream(inputBuffer);
        long[] times = new long[size];
        int[] nanos = new int[size];

        if (encoding.getKind().equals(PixelsProto.ColumnEncoding.Kind.RUNLENGTH)) {
            RunLenIntDecoder decoder = new RunLenIntDecoder(inputStream, false);
            for (int i = 0; i < size; i++) {
                times[i] = decoder.next();
            }
            for (int i = 0; i < size; i++) {
                nanos[i] = (int) decoder.next();
            }
        }
        else {
            for (int i = 0; i < size; i++) {
                times[i] = inputStream.readLong();
                nanos[i] = inputStream.readInt();
            }
        }

        for (int i = 0; i < size; i++) {
            Timestamp timestamp = new Timestamp(times[i]);
            timestamp.setNanos(nanos[i]);
            vector.add(timestamp);
        }

        inputStream.close();
        inputBuffer.release();
    }
}
