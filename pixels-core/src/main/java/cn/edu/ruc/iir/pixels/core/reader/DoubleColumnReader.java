package cn.edu.ruc.iir.pixels.core.reader;

import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.utils.EncodingUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;

import java.io.IOException;

/**
 * pixels
 *
 * @author guodong
 */
public class DoubleColumnReader
        extends ColumnReader
{
    private final EncodingUtils encodingUtils;

    public DoubleColumnReader(TypeDescription type)
    {
        super(type);
        this.encodingUtils = new EncodingUtils();
    }

    @Override
    public double[] readDoubles(byte[] input, int num) throws IOException
    {
        ByteBuf inputBuffer = Unpooled.copiedBuffer(input);
        ByteBufInputStream inputStream = new ByteBufInputStream(inputBuffer);
        double[] values = new double[num];
        for (int i = 0; i < num; i++) {
            values[i] = Double.longBitsToDouble(encodingUtils.readLongLE(inputStream));
        }
        inputStream.close();
        inputBuffer.release();
        return values;
    }
}
