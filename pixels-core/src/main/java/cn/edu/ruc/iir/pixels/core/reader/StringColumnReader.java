package cn.edu.ruc.iir.pixels.core.reader;

import cn.edu.ruc.iir.pixels.core.PixelsProto;
import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.encoding.RunLenIntDecoder;
import cn.edu.ruc.iir.pixels.core.utils.DynamicIntArray;
import cn.edu.ruc.iir.pixels.core.vector.ColumnVector;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.nio.charset.Charset;

/**
 * pixels
 *
 * @author guodong
 */
public class StringColumnReader
        extends ColumnReader
{
    private ByteBuf inputBuffer = null;

    StringColumnReader(TypeDescription type)
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
        if (offset == 0) {
            if (inputBuffer != null) {
                inputBuffer.release();
            }
            inputBuffer = Unpooled.wrappedBuffer(input);
        }
        // if dictionary encoded
        ByteBuf contentBuf;
        if (encoding.getKind().equals(PixelsProto.ColumnEncoding.Kind.DICTIONARY)) {
            // read offsets
            inputBuffer.markReaderIndex();
            inputBuffer.skipBytes(input.length - 3 * Integer.BYTES);
            int originsOffset = inputBuffer.readInt();
            int startsOffset = inputBuffer.readInt();
            int ordersOffset = inputBuffer.readInt();
            inputBuffer.resetReaderIndex();
            // read buffers
            contentBuf = inputBuffer.slice(0, originsOffset);
            ByteBuf originBuf = inputBuffer.slice(originsOffset, startsOffset - originsOffset);
            ByteBuf startsBuf = inputBuffer.slice(startsOffset, ordersOffset - startsOffset);
            ByteBuf ordersBuf = inputBuffer.slice(ordersOffset, input.length - ordersOffset);
            // fill starts array
            int originNum = 0;
            DynamicIntArray startsArray = new DynamicIntArray();
            RunLenIntDecoder startsDecoder = new RunLenIntDecoder(new ByteBufInputStream(startsBuf), false);
            while (startsDecoder.hasNext()) {
                startsArray.add((int) startsDecoder.next());
                originNum++;
            }
            // fill orders array
            RunLenIntDecoder ordersDecoder = new RunLenIntDecoder(new ByteBufInputStream(ordersBuf), false);
            int[] orders = new int[originNum];
            for (int i = 0; i < originNum && ordersDecoder.hasNext(); i++) {
                orders[i] = (int) ordersDecoder.next();
            }
            // content decoder
            RunLenIntDecoder contentDecoder = new RunLenIntDecoder(new ByteBufInputStream(contentBuf), false);
            // read original bytes
            for (int i = 0; i < size; i++) {
                int originId = orders[(int) contentDecoder.next()];
                int tmpLen = startsArray.get(originId + 1) - startsArray.get(originId);
                byte[] tmpBytes = new byte[tmpLen];
                originBuf.getBytes(startsArray.get(originId), tmpBytes);
                vector.add(new String(tmpBytes, Charset.forName("UTF-8")));
            }
        }
        // if un-encoded
        else {
            // read lens field offset
            inputBuffer.markReaderIndex();
            inputBuffer.skipBytes(input.length - Integer.BYTES);
            int lensOffset = inputBuffer.readInt();
            inputBuffer.resetReaderIndex();
            // read strings
            contentBuf = inputBuffer.slice(0, lensOffset);
            // read lens field
            ByteBuf lensBuf = inputBuffer.slice(lensOffset, input.length - Integer.BYTES - lensOffset);
            RunLenIntDecoder lensDecoder = new RunLenIntDecoder(new ByteBufInputStream(lensBuf), false);
            // read values
            for (int i = 0; i < size; i++) {
                int len = (int) lensDecoder.next();
                CharSequence str = contentBuf.readCharSequence(len, Charset.forName("UTF-8"));
                vector.add(str.toString());
            }
        }
    }
}
