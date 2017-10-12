package cn.edu.ruc.iir.pixels.core.utils;

import java.io.IOException;
import java.io.OutputStream;

/**
 * pixels
 *
 * @author guodong
 */
public class EncodingUtils
{
    private final static int BUFFER_SIZE = 64;
    private final byte[] writeBuffer;

    public EncodingUtils()
    {
        this.writeBuffer = new byte[BUFFER_SIZE];
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

    private void writeLongBE(OutputStream output, long[] input, int offset, int numHops, int numBytes) throws IOException {

        switch (numBytes) {
            case 1:
                writeBuffer[0] = (byte) (input[offset + 0] & 255);
                writeBuffer[1] = (byte) (input[offset + 1] & 255);
                writeBuffer[2] = (byte) (input[offset + 2] & 255);
                writeBuffer[3] = (byte) (input[offset + 3] & 255);
                writeBuffer[4] = (byte) (input[offset + 4] & 255);
                writeBuffer[5] = (byte) (input[offset + 5] & 255);
                writeBuffer[6] = (byte) (input[offset + 6] & 255);
                writeBuffer[7] = (byte) (input[offset + 7] & 255);
                break;
            case 2:
                writeLongBE2(output, input[offset + 0], 0);
                writeLongBE2(output, input[offset + 1], 2);
                writeLongBE2(output, input[offset + 2], 4);
                writeLongBE2(output, input[offset + 3], 6);
                writeLongBE2(output, input[offset + 4], 8);
                writeLongBE2(output, input[offset + 5], 10);
                writeLongBE2(output, input[offset + 6], 12);
                writeLongBE2(output, input[offset + 7], 14);
                break;
            case 3:
                writeLongBE3(output, input[offset + 0], 0);
                writeLongBE3(output, input[offset + 1], 3);
                writeLongBE3(output, input[offset + 2], 6);
                writeLongBE3(output, input[offset + 3], 9);
                writeLongBE3(output, input[offset + 4], 12);
                writeLongBE3(output, input[offset + 5], 15);
                writeLongBE3(output, input[offset + 6], 18);
                writeLongBE3(output, input[offset + 7], 21);
                break;
            case 4:
                writeLongBE4(output, input[offset + 0], 0);
                writeLongBE4(output, input[offset + 1], 4);
                writeLongBE4(output, input[offset + 2], 8);
                writeLongBE4(output, input[offset + 3], 12);
                writeLongBE4(output, input[offset + 4], 16);
                writeLongBE4(output, input[offset + 5], 20);
                writeLongBE4(output, input[offset + 6], 24);
                writeLongBE4(output, input[offset + 7], 28);
                break;
            case 5:
                writeLongBE5(output, input[offset + 0], 0);
                writeLongBE5(output, input[offset + 1], 5);
                writeLongBE5(output, input[offset + 2], 10);
                writeLongBE5(output, input[offset + 3], 15);
                writeLongBE5(output, input[offset + 4], 20);
                writeLongBE5(output, input[offset + 5], 25);
                writeLongBE5(output, input[offset + 6], 30);
                writeLongBE5(output, input[offset + 7], 35);
                break;
            case 6:
                writeLongBE6(output, input[offset + 0], 0);
                writeLongBE6(output, input[offset + 1], 6);
                writeLongBE6(output, input[offset + 2], 12);
                writeLongBE6(output, input[offset + 3], 18);
                writeLongBE6(output, input[offset + 4], 24);
                writeLongBE6(output, input[offset + 5], 30);
                writeLongBE6(output, input[offset + 6], 36);
                writeLongBE6(output, input[offset + 7], 42);
                break;
            case 7:
                writeLongBE7(output, input[offset + 0], 0);
                writeLongBE7(output, input[offset + 1], 7);
                writeLongBE7(output, input[offset + 2], 14);
                writeLongBE7(output, input[offset + 3], 21);
                writeLongBE7(output, input[offset + 4], 28);
                writeLongBE7(output, input[offset + 5], 35);
                writeLongBE7(output, input[offset + 6], 42);
                writeLongBE7(output, input[offset + 7], 49);
                break;
            case 8:
                writeLongBE8(output, input[offset + 0], 0);
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
        writeBuffer[wbOffset + 0] =  (byte) (val >>> 8);
        writeBuffer[wbOffset + 1] =  (byte) (val >>> 0);
    }

    private void writeLongBE3(OutputStream output, long val, int wbOffset) {
        writeBuffer[wbOffset + 0] =  (byte) (val >>> 16);
        writeBuffer[wbOffset + 1] =  (byte) (val >>> 8);
        writeBuffer[wbOffset + 2] =  (byte) (val >>> 0);
    }

    private void writeLongBE4(OutputStream output, long val, int wbOffset) {
        writeBuffer[wbOffset + 0] =  (byte) (val >>> 24);
        writeBuffer[wbOffset + 1] =  (byte) (val >>> 16);
        writeBuffer[wbOffset + 2] =  (byte) (val >>> 8);
        writeBuffer[wbOffset + 3] =  (byte) (val >>> 0);
    }

    private void writeLongBE5(OutputStream output, long val, int wbOffset) {
        writeBuffer[wbOffset + 0] =  (byte) (val >>> 32);
        writeBuffer[wbOffset + 1] =  (byte) (val >>> 24);
        writeBuffer[wbOffset + 2] =  (byte) (val >>> 16);
        writeBuffer[wbOffset + 3] =  (byte) (val >>> 8);
        writeBuffer[wbOffset + 4] =  (byte) (val >>> 0);
    }

    private void writeLongBE6(OutputStream output, long val, int wbOffset) {
        writeBuffer[wbOffset + 0] =  (byte) (val >>> 40);
        writeBuffer[wbOffset + 1] =  (byte) (val >>> 32);
        writeBuffer[wbOffset + 2] =  (byte) (val >>> 24);
        writeBuffer[wbOffset + 3] =  (byte) (val >>> 16);
        writeBuffer[wbOffset + 4] =  (byte) (val >>> 8);
        writeBuffer[wbOffset + 5] =  (byte) (val >>> 0);
    }

    private void writeLongBE7(OutputStream output, long val, int wbOffset) {
        writeBuffer[wbOffset + 0] =  (byte) (val >>> 48);
        writeBuffer[wbOffset + 1] =  (byte) (val >>> 40);
        writeBuffer[wbOffset + 2] =  (byte) (val >>> 32);
        writeBuffer[wbOffset + 3] =  (byte) (val >>> 24);
        writeBuffer[wbOffset + 4] =  (byte) (val >>> 16);
        writeBuffer[wbOffset + 5] =  (byte) (val >>> 8);
        writeBuffer[wbOffset + 6] =  (byte) (val >>> 0);
    }

    private void writeLongBE8(OutputStream output, long val, int wbOffset) {
        writeBuffer[wbOffset + 0] =  (byte) (val >>> 56);
        writeBuffer[wbOffset + 1] =  (byte) (val >>> 48);
        writeBuffer[wbOffset + 2] =  (byte) (val >>> 40);
        writeBuffer[wbOffset + 3] =  (byte) (val >>> 32);
        writeBuffer[wbOffset + 4] =  (byte) (val >>> 24);
        writeBuffer[wbOffset + 5] =  (byte) (val >>> 16);
        writeBuffer[wbOffset + 6] =  (byte) (val >>> 8);
        writeBuffer[wbOffset + 7] =  (byte) (val >>> 0);
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
}
