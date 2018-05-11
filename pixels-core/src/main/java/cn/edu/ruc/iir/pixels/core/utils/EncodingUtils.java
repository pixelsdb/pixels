package cn.edu.ruc.iir.pixels.core.utils;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;

/**
 * pixels
 *
 * @author guodong
 */
public class EncodingUtils
{
    private final static int BUFFER_SIZE = 64;
    private final byte[] writeBuffer;
    private final byte[] readBuffer;

    private enum FixedBitSizes {
        ONE, TWO, THREE, FOUR, FIVE, SIX, SEVEN, EIGHT, NINE, TEN, ELEVEN, TWELVE,
        THIRTEEN, FOURTEEN, FIFTEEN, SIXTEEN, SEVENTEEN, EIGHTEEN, NINETEEN,
        TWENTY, TWENTYONE, TWENTYTWO, TWENTYTHREE, TWENTYFOUR, TWENTYSIX,
        TWENTYEIGHT, THIRTY, THIRTYTWO, FORTY, FORTYEIGHT, FIFTYSIX, SIXTYFOUR;
    }

    public EncodingUtils()
    {
        this.writeBuffer = new byte[BUFFER_SIZE];
        this.readBuffer = new byte[BUFFER_SIZE];
    }

    public void unrolledBitPack1(long[] input, int offset, int len,
                                  OutputStream output) throws IOException
    {
        final int numHops = 8;
        final int remainder = len % numHops;
        final int endOffset = offset + len;
        final int endUnroll = endOffset - remainder;
        int val = 0;
        for (int i = offset; i < endUnroll; i = i + numHops) {
            val = (int) (val | ((input[i] & 1) << 7)
                    | ((input[i + 1] & 1) << 6)
                    | ((input[i + 2] & 1) << 5)
                    | ((input[i + 3] & 1) << 4)
                    | ((input[i + 4] & 1) << 3)
                    | ((input[i + 5] & 1) << 2)
                    | ((input[i + 6] & 1) << 1)
                    | (input[i + 7]) & 1);
            output.write(val);
            val = 0;
        }

        if (remainder > 0) {
            int startShift = 7;
            for (int i = endUnroll; i < endOffset; i++) {
                val = (int) (val | (input[i] & 1) << startShift);
                startShift -= 1;
            }
            output.write(val);
        }
    }

    public void unrolledBitPack2(long[] input, int offset, int len,
                                  OutputStream output) throws IOException {
        final int numHops = 4;
        final int remainder = len % numHops;
        final int endOffset = offset + len;
        final int endUnroll = endOffset - remainder;
        int val = 0;
        for (int i = offset; i < endUnroll; i = i + numHops) {
            val = (int) (val | ((input[i] & 3) << 6)
                    | ((input[i + 1] & 3) << 4)
                    | ((input[i + 2] & 3) << 2)
                    | (input[i + 3]) & 3);
            output.write(val);
            val = 0;
        }

        if (remainder > 0) {
            int startShift = 6;
            for (int i = endUnroll; i < endOffset; i++) {
                val = (int) (val | (input[i] & 3) << startShift);
                startShift -= 2;
            }
            output.write(val);
        }
    }

    public void unrolledBitPack4(long[] input, int offset, int len,
                                  OutputStream output) throws IOException {
        final int numHops = 2;
        final int remainder = len % numHops;
        final int endOffset = offset + len;
        final int endUnroll = endOffset - remainder;
        int val = 0;
        for (int i = offset; i < endUnroll; i = i + numHops) {
            val = (int) (val | ((input[i] & 15) << 4) | (input[i + 1]) & 15);
            output.write(val);
            val = 0;
        }

        if (remainder > 0) {
            int startShift = 4;
            for (int i = endUnroll; i < endOffset; i++) {
                val = (int) (val | (input[i] & 15) << startShift);
                startShift -= 4;
            }
            output.write(val);
        }
    }

    public void unrolledBitPack8(long[] input, int offset, int len,
                                  OutputStream output) throws IOException {
        unrolledBitPackBytes(input, offset, len, output, 1);
    }

    public void unrolledBitPack16(long[] input, int offset, int len,
                                   OutputStream output) throws IOException {
        unrolledBitPackBytes(input, offset, len, output, 2);
    }

    public void unrolledBitPack24(long[] input, int offset, int len,
                                   OutputStream output) throws IOException {
        unrolledBitPackBytes(input, offset, len, output, 3);
    }

    public void unrolledBitPack32(long[] input, int offset, int len,
                                   OutputStream output) throws IOException {
        unrolledBitPackBytes(input, offset, len, output, 4);
    }

    public void unrolledBitPack40(long[] input, int offset, int len,
                                   OutputStream output) throws IOException {
        unrolledBitPackBytes(input, offset, len, output, 5);
    }

    public void unrolledBitPack48(long[] input, int offset, int len,
                                   OutputStream output) throws IOException {
        unrolledBitPackBytes(input, offset, len, output, 6);
    }

    public void unrolledBitPack56(long[] input, int offset, int len,
                                   OutputStream output) throws IOException {
        unrolledBitPackBytes(input, offset, len, output, 7);
    }

    public void unrolledBitPack64(long[] input, int offset, int len,
                                   OutputStream output) throws IOException {
        unrolledBitPackBytes(input, offset, len, output, 8);
    }

    private void unrolledBitPackBytes(long[] input, int offset, int len, OutputStream output, int numBytes) throws IOException {
        final int numHops = 8;
        final int remainder = len % numHops;
        final int endOffset = offset + len;
        final int endUnroll = endOffset - remainder;
        int i = offset;
        for (; i < endUnroll; i = i + numHops) {
            writeLongBE(output, input, i, numHops, numBytes);
        }

        if (remainder > 0) {
            writeRemainingLongs(output, i, input, remainder, numBytes);
        }
    }

    public void writeLongLE(OutputStream output, long value) throws IOException
    {
        writeBuffer[0] = (byte) ((value)  & 0xff);
        writeBuffer[1] = (byte) ((value >> 8)  & 0xff);
        writeBuffer[2] = (byte) ((value >> 16) & 0xff);
        writeBuffer[3] = (byte) ((value >> 24) & 0xff);
        writeBuffer[4] = (byte) ((value >> 32) & 0xff);
        writeBuffer[5] = (byte) ((value >> 40) & 0xff);
        writeBuffer[6] = (byte) ((value >> 48) & 0xff);
        writeBuffer[7] = (byte) ((value >> 56) & 0xff);
        output.write(writeBuffer, 0, 8);
    }

    public long readLongLE(byte[] inputBytes)
    {
        return (((inputBytes[0] & 0xff))
                + ((inputBytes[1] & 0xff) << 8)
                + ((inputBytes[2] & 0xff) << 16)
                + ((long) (inputBytes[3] & 0xff) << 24)
                + ((long) (inputBytes[4] & 0xff) << 32)
                + ((long) (inputBytes[5] & 0xff) << 40)
                + ((long) (inputBytes[6] & 0xff) << 48)
                + ((long) (inputBytes[7] & 0xff) << 56));
    }

    public void writeFloat(OutputStream output, float value) throws IOException
    {
        int ser = Float.floatToIntBits(value);
        writeBuffer[0] = (byte) ((ser)  & 0xff);
        writeBuffer[1] = (byte) ((ser >> 8)  & 0xff);
        writeBuffer[2] = (byte) ((ser >> 16) & 0xff);
        writeBuffer[3] = (byte) ((ser >> 24) & 0xff);
        output.write(writeBuffer, 0, 4);
    }

    public float readFloat(byte[] inputBytes)
    {
        int value = (((inputBytes[0] & 0xff))
                + ((inputBytes[1] & 0xff) << 8)
                + ((inputBytes[2] & 0xff) << 16)
                + ((inputBytes[3] & 0xff) << 24)
        );
        return Float.intBitsToFloat(value);
    }

    private void readFully(final InputStream in, final byte[] buffer, final int off, final int len)
            throws IOException
    {
        int n = 0;
        while (n < len) {
            int count = in.read(buffer, off + n, len - n);
            if (count < 0) {
                throw new EOFException("Read past EOF for " + in);
            }
            n += count;
        }
    }

    private void writeLongBE(OutputStream output, long[] input, int offset, int numHops, int numBytes) throws IOException {

        switch (numBytes) {
            case 1:
                writeBuffer[0] = (byte) (input[offset] & 255);
                writeBuffer[1] = (byte) (input[offset + 1] & 255);
                writeBuffer[2] = (byte) (input[offset + 2] & 255);
                writeBuffer[3] = (byte) (input[offset + 3] & 255);
                writeBuffer[4] = (byte) (input[offset + 4] & 255);
                writeBuffer[5] = (byte) (input[offset + 5] & 255);
                writeBuffer[6] = (byte) (input[offset + 6] & 255);
                writeBuffer[7] = (byte) (input[offset + 7] & 255);
                break;
            case 2:
                writeLongBE2(output, input[offset], 0);
                writeLongBE2(output, input[offset + 1], 2);
                writeLongBE2(output, input[offset + 2], 4);
                writeLongBE2(output, input[offset + 3], 6);
                writeLongBE2(output, input[offset + 4], 8);
                writeLongBE2(output, input[offset + 5], 10);
                writeLongBE2(output, input[offset + 6], 12);
                writeLongBE2(output, input[offset + 7], 14);
                break;
            case 3:
                writeLongBE3(output, input[offset], 0);
                writeLongBE3(output, input[offset + 1], 3);
                writeLongBE3(output, input[offset + 2], 6);
                writeLongBE3(output, input[offset + 3], 9);
                writeLongBE3(output, input[offset + 4], 12);
                writeLongBE3(output, input[offset + 5], 15);
                writeLongBE3(output, input[offset + 6], 18);
                writeLongBE3(output, input[offset + 7], 21);
                break;
            case 4:
                writeLongBE4(output, input[offset], 0);
                writeLongBE4(output, input[offset + 1], 4);
                writeLongBE4(output, input[offset + 2], 8);
                writeLongBE4(output, input[offset + 3], 12);
                writeLongBE4(output, input[offset + 4], 16);
                writeLongBE4(output, input[offset + 5], 20);
                writeLongBE4(output, input[offset + 6], 24);
                writeLongBE4(output, input[offset + 7], 28);
                break;
            case 5:
                writeLongBE5(output, input[offset], 0);
                writeLongBE5(output, input[offset + 1], 5);
                writeLongBE5(output, input[offset + 2], 10);
                writeLongBE5(output, input[offset + 3], 15);
                writeLongBE5(output, input[offset + 4], 20);
                writeLongBE5(output, input[offset + 5], 25);
                writeLongBE5(output, input[offset + 6], 30);
                writeLongBE5(output, input[offset + 7], 35);
                break;
            case 6:
                writeLongBE6(output, input[offset], 0);
                writeLongBE6(output, input[offset + 1], 6);
                writeLongBE6(output, input[offset + 2], 12);
                writeLongBE6(output, input[offset + 3], 18);
                writeLongBE6(output, input[offset + 4], 24);
                writeLongBE6(output, input[offset + 5], 30);
                writeLongBE6(output, input[offset + 6], 36);
                writeLongBE6(output, input[offset + 7], 42);
                break;
            case 7:
                writeLongBE7(output, input[offset], 0);
                writeLongBE7(output, input[offset + 1], 7);
                writeLongBE7(output, input[offset + 2], 14);
                writeLongBE7(output, input[offset + 3], 21);
                writeLongBE7(output, input[offset + 4], 28);
                writeLongBE7(output, input[offset + 5], 35);
                writeLongBE7(output, input[offset + 6], 42);
                writeLongBE7(output, input[offset + 7], 49);
                break;
            case 8:
                writeLongBE8(output, input[offset], 0);
                writeLongBE8(output, input[offset + 1], 8);
                writeLongBE8(output, input[offset + 2], 16);
                writeLongBE8(output, input[offset + 3], 24);
                writeLongBE8(output, input[offset + 4], 32);
                writeLongBE8(output, input[offset + 5], 40);
                writeLongBE8(output, input[offset + 6], 48);
                writeLongBE8(output, input[offset + 7], 56);
                break;
            default:
                break;
        }

        final int toWrite = numHops * numBytes;
        output.write(writeBuffer, 0, toWrite);
    }

    private void writeLongBE2(OutputStream output, long val, int wbOffset) {
        writeBuffer[wbOffset] =  (byte) (val >>> 8);
        writeBuffer[wbOffset + 1] =  (byte) (val);
    }

    private void writeLongBE3(OutputStream output, long val, int wbOffset) {
        writeBuffer[wbOffset] =  (byte) (val >>> 16);
        writeBuffer[wbOffset + 1] =  (byte) (val >>> 8);
        writeBuffer[wbOffset + 2] =  (byte) (val);
    }

    private void writeLongBE4(OutputStream output, long val, int wbOffset) {
        writeBuffer[wbOffset] =  (byte) (val >>> 24);
        writeBuffer[wbOffset + 1] =  (byte) (val >>> 16);
        writeBuffer[wbOffset + 2] =  (byte) (val >>> 8);
        writeBuffer[wbOffset + 3] =  (byte) (val);
    }

    private void writeLongBE5(OutputStream output, long val, int wbOffset) {
        writeBuffer[wbOffset] =  (byte) (val >>> 32);
        writeBuffer[wbOffset + 1] =  (byte) (val >>> 24);
        writeBuffer[wbOffset + 2] =  (byte) (val >>> 16);
        writeBuffer[wbOffset + 3] =  (byte) (val >>> 8);
        writeBuffer[wbOffset + 4] =  (byte) (val);
    }

    private void writeLongBE6(OutputStream output, long val, int wbOffset) {
        writeBuffer[wbOffset] =  (byte) (val >>> 40);
        writeBuffer[wbOffset + 1] =  (byte) (val >>> 32);
        writeBuffer[wbOffset + 2] =  (byte) (val >>> 24);
        writeBuffer[wbOffset + 3] =  (byte) (val >>> 16);
        writeBuffer[wbOffset + 4] =  (byte) (val >>> 8);
        writeBuffer[wbOffset + 5] =  (byte) (val);
    }

    private void writeLongBE7(OutputStream output, long val, int wbOffset) {
        writeBuffer[wbOffset] =  (byte) (val >>> 48);
        writeBuffer[wbOffset + 1] =  (byte) (val >>> 40);
        writeBuffer[wbOffset + 2] =  (byte) (val >>> 32);
        writeBuffer[wbOffset + 3] =  (byte) (val >>> 24);
        writeBuffer[wbOffset + 4] =  (byte) (val >>> 16);
        writeBuffer[wbOffset + 5] =  (byte) (val >>> 8);
        writeBuffer[wbOffset + 6] =  (byte) (val);
    }

    private void writeLongBE8(OutputStream output, long val, int wbOffset) {
        writeBuffer[wbOffset] =  (byte) (val >>> 56);
        writeBuffer[wbOffset + 1] =  (byte) (val >>> 48);
        writeBuffer[wbOffset + 2] =  (byte) (val >>> 40);
        writeBuffer[wbOffset + 3] =  (byte) (val >>> 32);
        writeBuffer[wbOffset + 4] =  (byte) (val >>> 24);
        writeBuffer[wbOffset + 5] =  (byte) (val >>> 16);
        writeBuffer[wbOffset + 6] =  (byte) (val >>> 8);
        writeBuffer[wbOffset + 7] =  (byte) (val);
    }

    private void writeRemainingLongs(OutputStream output, int offset, long[] input, int remainder,
                                     int numBytes) throws IOException {
        final int numHops = remainder;

        int idx = 0;
        switch (numBytes) {
            case 1:
                while (remainder > 0) {
                    writeBuffer[idx] = (byte) (input[offset + idx] & 255);
                    remainder--;
                    idx++;
                }
                break;
            case 2:
                while (remainder > 0) {
                    writeLongBE2(output, input[offset + idx], idx * 2);
                    remainder--;
                    idx++;
                }
                break;
            case 3:
                while (remainder > 0) {
                    writeLongBE3(output, input[offset + idx], idx * 3);
                    remainder--;
                    idx++;
                }
                break;
            case 4:
                while (remainder > 0) {
                    writeLongBE4(output, input[offset + idx], idx * 4);
                    remainder--;
                    idx++;
                }
                break;
            case 5:
                while (remainder > 0) {
                    writeLongBE5(output, input[offset + idx], idx * 5);
                    remainder--;
                    idx++;
                }
                break;
            case 6:
                while (remainder > 0) {
                    writeLongBE6(output, input[offset + idx], idx * 6);
                    remainder--;
                    idx++;
                }
                break;
            case 7:
                while (remainder > 0) {
                    writeLongBE7(output, input[offset + idx], idx * 7);
                    remainder--;
                    idx++;
                }
                break;
            case 8:
                while (remainder > 0) {
                    writeLongBE8(output, input[offset + idx], idx * 8);
                    remainder--;
                    idx++;
                }
                break;
            default:
                break;
        }

        final int toWrite = numHops * numBytes;
        output.write(writeBuffer, 0, toWrite);
    }

    public void unrolledUnPack1(long[] buffer, int offset, int len,
                                 InputStream input) throws IOException {
        final int numHops = 8;
        final int remainder = len % numHops;
        final int endOffset = offset + len;
        final int endUnroll = endOffset - remainder;
        int val = 0;
        for (int i = offset; i < endUnroll; i = i + numHops) {
            val = input.read();
            buffer[i] = (val >>> 7) & 1;
            buffer[i + 1] = (val >>> 6) & 1;
            buffer[i + 2] = (val >>> 5) & 1;
            buffer[i + 3] = (val >>> 4) & 1;
            buffer[i + 4] = (val >>> 3) & 1;
            buffer[i + 5] = (val >>> 2) & 1;
            buffer[i + 6] = (val >>> 1) & 1;
            buffer[i + 7] = val & 1;
        }

        if (remainder > 0) {
            int startShift = 7;
            val = input.read();
            for (int i = endUnroll; i < endOffset; i++) {
                buffer[i] = (val >>> startShift) & 1;
                startShift -= 1;
            }
        }
    }

    public void unrolledUnPack2(long[] buffer, int offset, int len,
                                 InputStream input) throws IOException {
        final int numHops = 4;
        final int remainder = len % numHops;
        final int endOffset = offset + len;
        final int endUnroll = endOffset - remainder;
        int val = 0;
        for (int i = offset; i < endUnroll; i = i + numHops) {
            val = input.read();
            buffer[i] = (val >>> 6) & 3;
            buffer[i + 1] = (val >>> 4) & 3;
            buffer[i + 2] = (val >>> 2) & 3;
            buffer[i + 3] = val & 3;
        }

        if (remainder > 0) {
            int startShift = 6;
            val = input.read();
            for (int i = endUnroll; i < endOffset; i++) {
                buffer[i] = (val >>> startShift) & 3;
                startShift -= 2;
            }
        }
    }

    public void unrolledUnPack4(long[] buffer, int offset, int len,
                                 InputStream input) throws IOException {
        final int numHops = 2;
        final int remainder = len % numHops;
        final int endOffset = offset + len;
        final int endUnroll = endOffset - remainder;
        int val = 0;
        for (int i = offset; i < endUnroll; i = i + numHops) {
            val = input.read();
            buffer[i] = (val >>> 4) & 15;
            buffer[i + 1] = val & 15;
        }

        if (remainder > 0) {
            int startShift = 4;
            val = input.read();
            for (int i = endUnroll; i < endOffset; i++) {
                buffer[i] = (val >>> startShift) & 15;
                startShift -= 4;
            }
        }
    }

    public void unrolledUnPack8(long[] buffer, int offset, int len,
                                 InputStream input) throws IOException {
        unrolledUnPackBytes(buffer, offset, len, input, 1);
    }

    public void unrolledUnPack16(long[] buffer, int offset, int len,
                                  InputStream input) throws IOException {
        unrolledUnPackBytes(buffer, offset, len, input, 2);
    }

    public void unrolledUnPack24(long[] buffer, int offset, int len,
                                  InputStream input) throws IOException {
        unrolledUnPackBytes(buffer, offset, len, input, 3);
    }

    public void unrolledUnPack32(long[] buffer, int offset, int len,
                                  InputStream input) throws IOException {
        unrolledUnPackBytes(buffer, offset, len, input, 4);
    }

    public void unrolledUnPack40(long[] buffer, int offset, int len,
                                  InputStream input) throws IOException {
        unrolledUnPackBytes(buffer, offset, len, input, 5);
    }

    public void unrolledUnPack48(long[] buffer, int offset, int len,
                                  InputStream input) throws IOException {
        unrolledUnPackBytes(buffer, offset, len, input, 6);
    }

    public void unrolledUnPack56(long[] buffer, int offset, int len,
                                  InputStream input) throws IOException {
        unrolledUnPackBytes(buffer, offset, len, input, 7);
    }

    public void unrolledUnPack64(long[] buffer, int offset, int len,
                                  InputStream input) throws IOException {
        unrolledUnPackBytes(buffer, offset, len, input, 8);
    }

    private void unrolledUnPackBytes(long[] buffer, int offset, int len, InputStream input, int numBytes)
            throws IOException {
        final int numHops = 8;
        final int remainder = len % numHops;
        final int endOffset = offset + len;
        final int endUnroll = endOffset - remainder;
        int i = offset;
        for (; i < endUnroll; i = i + numHops) {
            readLongBE(input, buffer, i, numHops, numBytes);
        }

        if (remainder > 0) {
            readRemainingLongs(buffer, i, input, remainder, numBytes);
        }
    }

    private void readRemainingLongs(long[] buffer, int offset, InputStream input, int remainder,
                                    int numBytes) throws IOException {
        final int toRead = remainder * numBytes;
        // bulk read to buffer
        int bytesRead = input.read(readBuffer, 0, toRead);
        while (bytesRead != toRead) {
            bytesRead += input.read(readBuffer, bytesRead, toRead - bytesRead);
        }

        int idx = 0;
        switch (numBytes) {
            case 1:
                while (remainder > 0) {
                    buffer[offset++] = readBuffer[idx] & 255;
                    remainder--;
                    idx++;
                }
                break;
            case 2:
                while (remainder > 0) {
                    buffer[offset++] = readLongBE2(input, idx * 2);
                    remainder--;
                    idx++;
                }
                break;
            case 3:
                while (remainder > 0) {
                    buffer[offset++] = readLongBE3(input, idx * 3);
                    remainder--;
                    idx++;
                }
                break;
            case 4:
                while (remainder > 0) {
                    buffer[offset++] = readLongBE4(input, idx * 4);
                    remainder--;
                    idx++;
                }
                break;
            case 5:
                while (remainder > 0) {
                    buffer[offset++] = readLongBE5(input, idx * 5);
                    remainder--;
                    idx++;
                }
                break;
            case 6:
                while (remainder > 0) {
                    buffer[offset++] = readLongBE6(input, idx * 6);
                    remainder--;
                    idx++;
                }
                break;
            case 7:
                while (remainder > 0) {
                    buffer[offset++] = readLongBE7(input, idx * 7);
                    remainder--;
                    idx++;
                }
                break;
            case 8:
                while (remainder > 0) {
                    buffer[offset++] = readLongBE8(input, idx * 8);
                    remainder--;
                    idx++;
                }
                break;
            default:
                break;
        }
    }

    private void readLongBE(InputStream in, long[] buffer, int start, int numHops, int numBytes)
            throws IOException {
        final int toRead = numHops * numBytes;
        // bulk read to buffer
        int bytesRead = in.read(readBuffer, 0, toRead);
        while (bytesRead != toRead) {
            bytesRead += in.read(readBuffer, bytesRead, toRead - bytesRead);
        }

        switch (numBytes) {
            case 1:
                buffer[start + 0] = readBuffer[0] & 255;
                buffer[start + 1] = readBuffer[1] & 255;
                buffer[start + 2] = readBuffer[2] & 255;
                buffer[start + 3] = readBuffer[3] & 255;
                buffer[start + 4] = readBuffer[4] & 255;
                buffer[start + 5] = readBuffer[5] & 255;
                buffer[start + 6] = readBuffer[6] & 255;
                buffer[start + 7] = readBuffer[7] & 255;
                break;
            case 2:
                buffer[start + 0] = readLongBE2(in, 0);
                buffer[start + 1] = readLongBE2(in, 2);
                buffer[start + 2] = readLongBE2(in, 4);
                buffer[start + 3] = readLongBE2(in, 6);
                buffer[start + 4] = readLongBE2(in, 8);
                buffer[start + 5] = readLongBE2(in, 10);
                buffer[start + 6] = readLongBE2(in, 12);
                buffer[start + 7] = readLongBE2(in, 14);
                break;
            case 3:
                buffer[start + 0] = readLongBE3(in, 0);
                buffer[start + 1] = readLongBE3(in, 3);
                buffer[start + 2] = readLongBE3(in, 6);
                buffer[start + 3] = readLongBE3(in, 9);
                buffer[start + 4] = readLongBE3(in, 12);
                buffer[start + 5] = readLongBE3(in, 15);
                buffer[start + 6] = readLongBE3(in, 18);
                buffer[start + 7] = readLongBE3(in, 21);
                break;
            case 4:
                buffer[start + 0] = readLongBE4(in, 0);
                buffer[start + 1] = readLongBE4(in, 4);
                buffer[start + 2] = readLongBE4(in, 8);
                buffer[start + 3] = readLongBE4(in, 12);
                buffer[start + 4] = readLongBE4(in, 16);
                buffer[start + 5] = readLongBE4(in, 20);
                buffer[start + 6] = readLongBE4(in, 24);
                buffer[start + 7] = readLongBE4(in, 28);
                break;
            case 5:
                buffer[start + 0] = readLongBE5(in, 0);
                buffer[start + 1] = readLongBE5(in, 5);
                buffer[start + 2] = readLongBE5(in, 10);
                buffer[start + 3] = readLongBE5(in, 15);
                buffer[start + 4] = readLongBE5(in, 20);
                buffer[start + 5] = readLongBE5(in, 25);
                buffer[start + 6] = readLongBE5(in, 30);
                buffer[start + 7] = readLongBE5(in, 35);
                break;
            case 6:
                buffer[start + 0] = readLongBE6(in, 0);
                buffer[start + 1] = readLongBE6(in, 6);
                buffer[start + 2] = readLongBE6(in, 12);
                buffer[start + 3] = readLongBE6(in, 18);
                buffer[start + 4] = readLongBE6(in, 24);
                buffer[start + 5] = readLongBE6(in, 30);
                buffer[start + 6] = readLongBE6(in, 36);
                buffer[start + 7] = readLongBE6(in, 42);
                break;
            case 7:
                buffer[start + 0] = readLongBE7(in, 0);
                buffer[start + 1] = readLongBE7(in, 7);
                buffer[start + 2] = readLongBE7(in, 14);
                buffer[start + 3] = readLongBE7(in, 21);
                buffer[start + 4] = readLongBE7(in, 28);
                buffer[start + 5] = readLongBE7(in, 35);
                buffer[start + 6] = readLongBE7(in, 42);
                buffer[start + 7] = readLongBE7(in, 49);
                break;
            case 8:
                buffer[start + 0] = readLongBE8(in, 0);
                buffer[start + 1] = readLongBE8(in, 8);
                buffer[start + 2] = readLongBE8(in, 16);
                buffer[start + 3] = readLongBE8(in, 24);
                buffer[start + 4] = readLongBE8(in, 32);
                buffer[start + 5] = readLongBE8(in, 40);
                buffer[start + 6] = readLongBE8(in, 48);
                buffer[start + 7] = readLongBE8(in, 56);
                break;
            default:
                break;
        }
    }

    private long readLongBE2(InputStream in, int rbOffset) {
        return (((readBuffer[rbOffset] & 255) << 8)
                + ((readBuffer[rbOffset + 1] & 255) << 0));
    }

    private long readLongBE3(InputStream in, int rbOffset) {
        return (((readBuffer[rbOffset] & 255) << 16)
                + ((readBuffer[rbOffset + 1] & 255) << 8)
                + ((readBuffer[rbOffset + 2] & 255) << 0));
    }

    private long readLongBE4(InputStream in, int rbOffset) {
        return (((long) (readBuffer[rbOffset] & 255) << 24)
                + ((readBuffer[rbOffset + 1] & 255) << 16)
                + ((readBuffer[rbOffset + 2] & 255) << 8)
                + ((readBuffer[rbOffset + 3] & 255) << 0));
    }

    private long readLongBE5(InputStream in, int rbOffset) {
        return (((long) (readBuffer[rbOffset] & 255) << 32)
                + ((long) (readBuffer[rbOffset + 1] & 255) << 24)
                + ((readBuffer[rbOffset + 2] & 255) << 16)
                + ((readBuffer[rbOffset + 3] & 255) << 8)
                + ((readBuffer[rbOffset + 4] & 255) << 0));
    }

    private long readLongBE6(InputStream in, int rbOffset) {
        return (((long) (readBuffer[rbOffset] & 255) << 40)
                + ((long) (readBuffer[rbOffset + 1] & 255) << 32)
                + ((long) (readBuffer[rbOffset + 2] & 255) << 24)
                + ((readBuffer[rbOffset + 3] & 255) << 16)
                + ((readBuffer[rbOffset + 4] & 255) << 8)
                + ((readBuffer[rbOffset + 5] & 255) << 0));
    }

    private long readLongBE7(InputStream in, int rbOffset) {
        return (((long) (readBuffer[rbOffset] & 255) << 48)
                + ((long) (readBuffer[rbOffset + 1] & 255) << 40)
                + ((long) (readBuffer[rbOffset + 2] & 255) << 32)
                + ((long) (readBuffer[rbOffset + 3] & 255) << 24)
                + ((readBuffer[rbOffset + 4] & 255) << 16)
                + ((readBuffer[rbOffset + 5] & 255) << 8)
                + ((readBuffer[rbOffset + 6] & 255) << 0));
    }

    private long readLongBE8(InputStream in, int rbOffset) {
        return (((long) (readBuffer[rbOffset] & 255) << 56)
                + ((long) (readBuffer[rbOffset + 1] & 255) << 48)
                + ((long) (readBuffer[rbOffset + 2] & 255) << 40)
                + ((long) (readBuffer[rbOffset + 3] & 255) << 32)
                + ((long) (readBuffer[rbOffset + 4] & 255) << 24)
                + ((readBuffer[rbOffset + 5] & 255) << 16)
                + ((readBuffer[rbOffset + 6] & 255) << 8)
                + ((readBuffer[rbOffset + 7] & 255) << 0));
    }

    public int encodeBitWidth(int n)
    {
        n = getClosestFixedBits(n);

        if (n >= 1 && n <= 24) {
            return n - 1;
        } else if (n > 24 && n <= 26) {
            return FixedBitSizes.TWENTYSIX.ordinal();
        } else if (n > 26 && n <= 28) {
            return FixedBitSizes.TWENTYEIGHT.ordinal();
        } else if (n > 28 && n <= 30) {
            return FixedBitSizes.THIRTY.ordinal();
        } else if (n > 30 && n <= 32) {
            return FixedBitSizes.THIRTYTWO.ordinal();
        } else if (n > 32 && n <= 40) {
            return FixedBitSizes.FORTY.ordinal();
        } else if (n > 40 && n <= 48) {
            return FixedBitSizes.FORTYEIGHT.ordinal();
        } else if (n > 48 && n <= 56) {
            return FixedBitSizes.FIFTYSIX.ordinal();
        } else {
            return FixedBitSizes.SIXTYFOUR.ordinal();
        }
    }

    public int decodeBitWidth(int n)
    {
        if (n >= FixedBitSizes.ONE.ordinal()
                && n <= FixedBitSizes.TWENTYFOUR.ordinal()) {
            return n + 1;
        } else if (n == FixedBitSizes.TWENTYSIX.ordinal()) {
            return 26;
        } else if (n == FixedBitSizes.TWENTYEIGHT.ordinal()) {
            return 28;
        } else if (n == FixedBitSizes.THIRTY.ordinal()) {
            return 30;
        } else if (n == FixedBitSizes.THIRTYTWO.ordinal()) {
            return 32;
        } else if (n == FixedBitSizes.FORTY.ordinal()) {
            return 40;
        } else if (n == FixedBitSizes.FORTYEIGHT.ordinal()) {
            return 48;
        } else if (n == FixedBitSizes.FIFTYSIX.ordinal()) {
            return 56;
        } else {
            return 64;
        }
    }

    public int getClosestFixedBits(int n)
    {
        if (n == 0) {
            return 1;
        }

        if (n >= 1 && n <= 24) {
            return n;
        } else if (n > 24 && n <= 26) {
            return 26;
        } else if (n > 26 && n <= 28) {
            return 28;
        } else if (n > 28 && n <= 30) {
            return 30;
        } else if (n > 30 && n <= 32) {
            return 32;
        } else if (n > 32 && n <= 40) {
            return 40;
        } else if (n > 40 && n <= 48) {
            return 48;
        } else if (n > 48 && n <= 56) {
            return 56;
        } else {
            return 64;
        }
    }

    private static ThreadLocal<CharsetEncoder> ENCODER_FACTORY =
            ThreadLocal.withInitial(() -> Charset.forName("UTF-8").newEncoder().
                    onMalformedInput(CodingErrorAction.REPORT).
                    onUnmappableCharacter(CodingErrorAction.REPORT));

    private static ThreadLocal<CharsetDecoder> DECODER_FACTORY =
            ThreadLocal.withInitial(() -> Charset.forName("UTF-8").newDecoder().
                    onMalformedInput(CodingErrorAction.REPORT).
                    onUnmappableCharacter(CodingErrorAction.REPORT));

    public static ByteBuffer encodeString(String string, boolean replace)
            throws CharacterCodingException
    {
        CharsetEncoder encoder = ENCODER_FACTORY.get();
        if (replace) {
            encoder.onMalformedInput(CodingErrorAction.REPLACE);
            encoder.onUnmappableCharacter(CodingErrorAction.REPLACE);
        }
        ByteBuffer bytes =
                encoder.encode(CharBuffer.wrap(string.toCharArray()));
        if (replace) {
            encoder.onMalformedInput(CodingErrorAction.REPORT);
            encoder.onUnmappableCharacter(CodingErrorAction.REPORT);
        }
        return bytes;
    }

    public static String decodeString(ByteBuffer bytes, boolean replace) throws CharacterCodingException
    {
        CharsetDecoder decoder = DECODER_FACTORY.get();
        if (replace) {
            decoder.onMalformedInput(CodingErrorAction.REPLACE);
            decoder.onUnmappableCharacter(CodingErrorAction.REPLACE);
        }
        String str = decoder.decode(bytes).toString();
        if (replace) {
            decoder.onMalformedInput(CodingErrorAction.REPORT);
            decoder.onUnmappableCharacter(CodingErrorAction.REPORT);
        }
        return str;
    }
}
