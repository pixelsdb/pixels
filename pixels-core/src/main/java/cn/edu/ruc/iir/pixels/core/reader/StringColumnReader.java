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
        ByteBuf inputBuffer = Unpooled.copiedBuffer(input);
        // if dictionary encoded
        if (encoding.getKind().equals(PixelsProto.ColumnEncoding.Kind.DICTIONARY)) {
            // read offsets
            inputBuffer.markReaderIndex();
            inputBuffer.skipBytes(input.length - 3 * Integer.BYTES);
            int originsOffset = inputBuffer.readInt();
            int startsOffset = inputBuffer.readInt();
            int ordersOffset = inputBuffer.readInt();
            inputBuffer.resetReaderIndex();
            // read buffers
            ByteBuf contentBuf = inputBuffer.slice(0, originsOffset);
            ByteBuf originBuf = inputBuffer.slice(originsOffset, startsOffset - originsOffset);
            ByteBuf startsBuf = inputBuffer.slice(startsOffset, ordersOffset - startsOffset);
            ByteBuf ordersBuf = inputBuffer.slice(ordersOffset, input.length - ordersOffset);
            int originNum = 0;
            DynamicIntArray startsArray = new DynamicIntArray();
            RunLenIntDecoder startsDecoder = new RunLenIntDecoder(new ByteBufInputStream(startsBuf), false);
            while (startsDecoder.hasNext()) {
                startsArray.add((int) startsDecoder.next());
                originNum++;
            }

            // read starts and orders
            RunLenIntDecoder ordersDecoder = new RunLenIntDecoder(new ByteBufInputStream(ordersBuf), false);
            int[] starts = new int[originNum];
            int[] orders = new int[originNum];
            for (int i = 0; i < originNum && ordersDecoder.hasNext(); i++) {
                starts[i] = startsArray.get(i);
                orders[i] = (int) ordersDecoder.next();
            }
            // read origins
            String[] origins = new String[originNum];
            for (int i = 0; i < originNum - 1; i++) {
                byte[] tmp = new byte[starts[i + 1] - starts[i]];
                originBuf.readBytes(tmp);
                origins[i] = new String(tmp, Charset.forName("UTF-8"));
            }
            int tmpLen = originBuf.readableBytes();
            byte[] tmp = new byte[tmpLen];
            originBuf.readBytes(tmp);
            origins[originNum - 1] = new String(tmp, Charset.forName("UTF-8"));

            // read encoded values
            int[] encodedValues = new int[size];
            RunLenIntDecoder decoder = new RunLenIntDecoder(new ByteBufInputStream(contentBuf), false);
            for (int i = 0; i < size; i++) {
                encodedValues[i] = (int) decoder.next();
            }

            // read original bytes
            for (int i = 0; i < size; i++) {
                vector.add(origins[orders[encodedValues[i + offset]]]);
            }
        }
        // if un-encoded
        else {
            int[] lens = new int[size];
            // read lens field offset
            inputBuffer.markReaderIndex();
            inputBuffer.skipBytes(input.length - Integer.BYTES);
            int lensOffset = inputBuffer.readInt();
            inputBuffer.resetReaderIndex();
            // read lens field
            ByteBuf lensBuf = inputBuffer.slice(lensOffset, input.length - Integer.BYTES - lensOffset);
            RunLenIntDecoder decoder = new RunLenIntDecoder(new ByteBufInputStream(lensBuf), false);
            for (int i = 0; i < size; i++) {
                lens[i] = (int) decoder.next();
            }
            // read strings
            ByteBuf contentBuf = inputBuffer.slice(0, lensOffset);
            // skip values before offset
            for (int i = 0; i < offset; i++) {
                contentBuf.readCharSequence(lens[i], Charset.forName("UTF-8"));
            }
            // read values
            for (int i = 0; i < size; i++) {
                CharSequence str = contentBuf.readCharSequence(lens[i], Charset.forName("UTF-8"));
                vector.add(str.toString());
            }
        }

        inputBuffer.release();
    }
}
