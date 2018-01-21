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
public class FloatColumnReader
        extends ColumnReader
{
    private final EncodingUtils encodingUtils;

    public FloatColumnReader(TypeDescription type)
    {
        super(type);
        this.encodingUtils = new EncodingUtils();
    }

    @Override
    public float[] readFloats(byte[] input, int num) throws IOException
    {
        ByteBuf inputBuffer = Unpooled.copiedBuffer(input);
        ByteBufInputStream inputStream = new ByteBufInputStream(inputBuffer);
        float[] values = new float[num];

        for (int i = 0; i < num; i++) {
            values[i] = encodingUtils.readFloat(inputStream);
        }

        inputStream.close();
        inputBuffer.release();
        return values;
    }
}
