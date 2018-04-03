package cn.edu.ruc.iir.pixels.core.reader;

import cn.edu.ruc.iir.pixels.core.PixelsProto;
import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.utils.EncodingUtils;
import cn.edu.ruc.iir.pixels.core.vector.ColumnVector;
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
    private ByteBuf inputBuffer = null;
    private ByteBufInputStream inputStream = null;

    FloatColumnReader(TypeDescription type)
    {
        super(type);
        this.encodingUtils = new EncodingUtils();
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
        if (offset == 0) {
            if (inputStream != null) {
                inputStream.close();
            }
            if (inputBuffer != null) {
                inputBuffer.release();
            }
            inputBuffer = Unpooled.copiedBuffer(input);
            inputStream = new ByteBufInputStream(inputBuffer);
        }
        for (int i = 0; i < size; i++) {
            vector.add(encodingUtils.readFloat(inputStream));
        }
    }
}
