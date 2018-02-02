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

import static com.google.common.base.Preconditions.checkArgument;

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
                     int offset, int size, int pixelStride, ColumnVector vector) throws IOException
    {
        ByteBuf inputBuffer = Unpooled.copiedBuffer(input);
        ByteBufInputStream inputStream = new ByteBufInputStream(inputBuffer);
        long[] times = new long[size];
        int[] nanos = new int[size];
        RunLenIntDecoder decoder = new RunLenIntDecoder(inputStream, false);

        if (encoding.getKind().equals(PixelsProto.ColumnEncoding.Kind.RUNLENGTH)) {
            int pixelOff = 0;
            while (pixelOff < size) {
                int i = 0;
                int pixelIdx = pixelOff;
                for (; decoder.hasNext() && i < pixelStride && pixelIdx < size; i++) {
                    times[pixelOff + i] = decoder.next();
                    checkArgument(times[pixelOff + i] == 1517551844468L,
                            "time current index : " + pixelOff + i + ", value: " + times[pixelOff + i]);
                    pixelIdx++;
                }
                i = 0;
                pixelIdx = pixelOff;
                for (; decoder.hasNext() && i < pixelStride && pixelIdx < size; i++) {
                    nanos[pixelOff + i] = (int) decoder.next();
                    checkArgument(times[pixelOff + i] == 1517551844468L,
                            "nanos current index : " + pixelOff + i);
                    pixelIdx++;
                }
                pixelOff += i;
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
            try {
                timestamp.setNanos(nanos[i]);
            }
            catch (IllegalArgumentException e) {
                e.printStackTrace();
                System.out.println("Nanos: " + nanos[i]);
            }
            vector.add(timestamp);
        }

        inputStream.close();
        inputBuffer.release();
    }
}
