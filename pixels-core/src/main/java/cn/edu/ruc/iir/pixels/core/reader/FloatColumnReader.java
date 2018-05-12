package cn.edu.ruc.iir.pixels.core.reader;

import cn.edu.ruc.iir.pixels.core.PixelsProto;
import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.utils.EncodingUtils;
import cn.edu.ruc.iir.pixels.core.vector.ColumnVector;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

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
                     int offset, int size, int pixelStride, ColumnVector vector)
    {
        if (offset == 0) {
            if (inputBuffer != null) {
                inputBuffer.release();
            }
            inputBuffer = Unpooled.wrappedBuffer(input);
        }
        for (int i = 0; i < size; i++) {
            byte[] inputBytes = new byte[4];
            inputBuffer.readBytes(inputBytes);
            vector.add(encodingUtils.readFloat(inputBytes));
        }
    }
}
