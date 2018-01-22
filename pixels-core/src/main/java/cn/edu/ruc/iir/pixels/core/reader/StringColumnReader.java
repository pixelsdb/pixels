package cn.edu.ruc.iir.pixels.core.reader;

import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.encoding.RunLenIntDecoder;
import cn.edu.ruc.iir.pixels.core.utils.DynamicIntArray;
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
    public StringColumnReader(TypeDescription type)
    {
        super(type);
    }

    @Override
    public String[] readStrings(byte[] input, boolean isEncoding, int num) throws IOException
    {
        ByteBuf inputBuffer = Unpooled.copiedBuffer(input);
        String[] values = new String[num];
        // if dictionary encoded
        if (isEncoding) {
            // read offsets
            inputBuffer.markReaderIndex();
            inputBuffer.skipBytes(input.length - 12);
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
            while (startsBuf.isReadable()) {
                startsArray.add(startsBuf.readInt());
                originNum++;
            }

            // read starts and orders
            int[] starts = new int[originNum];
            int[] orders = new int[originNum];
            for (int i = 0; i < originNum; i++) {
                starts[i] = startsArray.get(i);
                orders[i] = ordersBuf.readInt();
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
            int[] encodedValues = new int[num];
            RunLenIntDecoder decoder = new RunLenIntDecoder(new ByteBufInputStream(contentBuf), false);
            for (int i = 0; i < num; i++) {
                encodedValues[i] = (int) decoder.next();
            }

            // read original bytes
            for (int i = 0; i < num; i++) {
                values[i] = origins[orders[encodedValues[i]]];
            }
        }
        // if un-encoded
        else {
            int[] lens = new int[num];
            // read lens field offset
            inputBuffer.markReaderIndex();
            inputBuffer.skipBytes(input.length - 4);
            int lensOffset = inputBuffer.readInt();
            inputBuffer.resetReaderIndex();
            // read lens field
            ByteBuf lensBuf = inputBuffer.slice(lensOffset, input.length - 4 - lensOffset);
            RunLenIntDecoder decoder = new RunLenIntDecoder(new ByteBufInputStream(lensBuf), false);
            for (int i = 0; i < num; i++) {
                lens[i] = (int) decoder.next();
            }
            // read strings
            ByteBuf contentBuf = inputBuffer.slice(0, lensOffset);
            for (int i = 0; i < num; i++) {
                CharSequence str = contentBuf.readCharSequence(lens[i], Charset.forName("UTF-8"));
                values[i] = str.toString();
            }
            lensBuf.release();
            contentBuf.release();
        }
        inputBuffer.release();

        return values;
    }
}
