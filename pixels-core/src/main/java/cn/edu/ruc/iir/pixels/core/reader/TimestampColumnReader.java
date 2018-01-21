package cn.edu.ruc.iir.pixels.core.reader;

import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.encoding.RunLenIntDecoder;
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

    @Override
    public Timestamp[] readTimestamps(byte[] input, boolean isEncoding, int num) throws IOException
    {
        ByteBuf inputBuffer = Unpooled.copiedBuffer(input);
        ByteBufInputStream inputStream = new ByteBufInputStream(inputBuffer);
        Timestamp[] values = new Timestamp[num];
        long[] times = new long[num];
        int[] nanos = new int[num];

        if (isEncoding) {
            RunLenIntDecoder decoder = new RunLenIntDecoder(inputStream, false);
            for (int i = 0; i < num; i++) {
                times[i] = decoder.next();
            }
            for (int i = 0; i < num; i++) {
                nanos[i] = (int) decoder.next();
            }
        }
        else {
            for (int i = 0; i < num; i++) {
                times[i] = inputStream.readLong();
                nanos[i] = inputStream.readInt();
            }
        }

        for (int i = 0; i < num; i++) {
            Timestamp timestamp = new Timestamp(times[i]);
            timestamp.setNanos(nanos[i]);
            values[i] = timestamp;
        }

        inputStream.close();
        inputBuffer.release();
        return values;
    }
}
