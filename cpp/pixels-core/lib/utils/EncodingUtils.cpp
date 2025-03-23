//
// Created by liyu on 3/21/23.
//

#include "utils/EncodingUtils.h"

int EncodingUtils::BUFFER_SIZE = 64;


EncodingUtils::EncodingUtils() {
    writeBuffer = new uint8_t[BUFFER_SIZE];
    readBuffer = new uint8_t[BUFFER_SIZE];
}

int EncodingUtils::decodeBitWidth(int n) {
    if (n >= FixedBitSizes::ONE && n <= FixedBitSizes::TWENTYFOUR) {
        return n + 1;
    }
    else if (n == FixedBitSizes::TWENTYSIX) {
        return 26;
    }
    else if (n == FixedBitSizes::TWENTYEIGHT) {
        return 28;
    }
    else if (n == FixedBitSizes::THIRTY) {
        return 30;
    }
    else if (n == FixedBitSizes::THIRTYTWO) {
        return 32;
    }
    else if (n == FixedBitSizes::FORTY) {
        return 40;
    }
    else if (n == FixedBitSizes::FORTYEIGHT) {
        return 48;
    }
    else if (n == FixedBitSizes::FIFTYSIX) {
        return 56;
    }
    else {
        return 64;
    }
}

void EncodingUtils::unrolledUnPack1(long *buffer, int offset, int len,
                                    const std::shared_ptr<ByteBuffer> &input) {
	int numHops = 8;
	int remainder = len % numHops;
	int endOffset = offset + len;
	int endUnroll = endOffset - remainder;
	int val = 0;
	for (int i = offset; i < endUnroll; i = i + numHops) {
		val = input->get();
		buffer[i] = (((uint32_t)val) >> 7) & 1;
		buffer[i + 1] = (((uint32_t)val) >> 6) & 1;
		buffer[i + 2] = (((uint32_t)val) >> 5) & 1;
		buffer[i + 3] = (((uint32_t)val) >> 4) & 1;
		buffer[i + 4] = (((uint32_t)val) >> 3) & 1;
		buffer[i + 5] = (((uint32_t)val) >> 2) & 1;
		buffer[i + 6] = (((uint32_t)val) >> 1) & 1;
		buffer[i + 7] = val & 1;
	}

	if (remainder > 0) {
		int startShift = 7;
		val = input->get();
		for (int i = endUnroll; i < endOffset; i++) {
			buffer[i] = (((uint32_t)val) >> startShift) & 1;
			startShift -= 1;
		}
	}
}

void EncodingUtils::unrolledUnPack2(long *buffer, int offset, int len,
                                    const std::shared_ptr<ByteBuffer> &input) {
	int numHops = 4;
	int remainder = len % numHops;
	int endOffset = offset + len;
	int endUnroll = endOffset - remainder;
	int val = 0;
	for (int i = offset; i < endUnroll; i = i + numHops) {
		val = input->get();
		buffer[i] = (((uint32_t)val) >> 6) & 3;
		buffer[i + 1] = (((uint32_t)val) >> 4) & 3;
		buffer[i + 2] = (((uint32_t)val) >> 2) & 3;
		buffer[i + 3] = val & 3;
	}

	if (remainder > 0) {
		int startShift = 6;
		val = input->get();
		for (int i = endUnroll; i < endOffset; i++) {
			buffer[i] = (((uint32_t)val) >> startShift) & 3;
			startShift -= 2;
		}
	}
}

void EncodingUtils::unrolledUnPack4(long *buffer, int offset, int len,
                                    const std::shared_ptr<ByteBuffer> &input) {
    int numHops = 2;
    int remainder = len % numHops;
    int endOffset = offset + len;
    int endUnroll = endOffset - remainder;
    int val = 0;
    for (int i = offset; i < endUnroll; i += numHops)
    {
        val = input->get();
        buffer[i] = ((uint32_t)val >> 4) & 15;
        buffer[i + 1] = val & 15;
    }

    if (remainder > 0)
    {
        int startShift = 4;
        val = input->get();
        for (int i = endUnroll; i < endOffset; i++)
        {
            buffer[i] = ((uint32_t)val >> startShift) & 15;
            startShift -= 4;
        }
    }
}

void EncodingUtils::unrolledUnPack8(long *buffer, int offset, int len,
                                    const std::shared_ptr<ByteBuffer> &input) {
    unrolledUnPackBytes(buffer, offset, len, input, 1);
}

void EncodingUtils::unrolledUnPack16(long *buffer, int offset, int len,
                                    const std::shared_ptr<ByteBuffer> &input) {
	unrolledUnPackBytes(buffer, offset, len, input, 2);
}

void EncodingUtils::unrolledUnPack24(long *buffer, int offset, int len,
                                    const std::shared_ptr<ByteBuffer> &input) {
	unrolledUnPackBytes(buffer, offset, len, input, 3);
}

void EncodingUtils::unrolledUnPack32(long *buffer, int offset, int len,
                                     const std::shared_ptr<ByteBuffer> &input) {
	unrolledUnPackBytes(buffer, offset, len, input, 4);
}

void EncodingUtils::unrolledUnPack40(long *buffer, int offset, int len,
                                     const std::shared_ptr<ByteBuffer> &input) {
	unrolledUnPackBytes(buffer, offset, len, input, 5);
}

void EncodingUtils::unrolledUnPack48(long *buffer, int offset, int len,
                                     const std::shared_ptr<ByteBuffer> &input) {
	unrolledUnPackBytes(buffer, offset, len, input, 6);
}

void EncodingUtils::unrolledUnPack56(long *buffer, int offset, int len,
                                     const std::shared_ptr<ByteBuffer> &input) {
	unrolledUnPackBytes(buffer, offset, len, input, 7);
}

void EncodingUtils::unrolledUnPack64(long *buffer, int offset, int len,
                                     const std::shared_ptr<ByteBuffer> &input) {
	unrolledUnPackBytes(buffer, offset, len, input, 8);
}

void EncodingUtils::unrolledUnPackBytes(long *buffer, int offset, int len,
                                        const std::shared_ptr<ByteBuffer> &input,
                                        int numBytes) {
    int numHops = 8;
    int remainder = len % numHops;
    int endOffset = offset + len;
    int endUnroll = endOffset - remainder;
    int i = offset;
    for (; i < endUnroll; i = i + numHops) {
        readLongBE(input, buffer, i, numHops, numBytes);
    }

    if (remainder > 0) {
        readRemainingLongs(input, buffer, i, remainder, numBytes);
    }
}

void EncodingUtils::readLongBE(const std::shared_ptr<ByteBuffer> &input, long *buffer, int start, int numHops,
                               int numBytes) {
    int toRead = numHops * numBytes;
    // bulk read to buffer
    int bytesRead = input->read(readBuffer, 0, toRead);
    while (bytesRead != toRead) {
        bytesRead += input->read(readBuffer, bytesRead, toRead - bytesRead);
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
            buffer[start + 0] = readLongBE2(0);
            buffer[start + 1] = readLongBE2(2);
            buffer[start + 2] = readLongBE2(4);
            buffer[start + 3] = readLongBE2(6);
            buffer[start + 4] = readLongBE2(8);
            buffer[start + 5] = readLongBE2(10);
            buffer[start + 6] = readLongBE2(12);
            buffer[start + 7] = readLongBE2(14);
            break;
        case 3:
            buffer[start + 0] = readLongBE3(0);
            buffer[start + 1] = readLongBE3(3);
            buffer[start + 2] = readLongBE3(6);
            buffer[start + 3] = readLongBE3(9);
            buffer[start + 4] = readLongBE3(12);
            buffer[start + 5] = readLongBE3(15);
            buffer[start + 6] = readLongBE3(18);
            buffer[start + 7] = readLongBE3(21);
            break;
        case 4:
            buffer[start + 0] = readLongBE4(0);
            buffer[start + 1] = readLongBE4(4);
            buffer[start + 2] = readLongBE4(8);
            buffer[start + 3] = readLongBE4(12);
            buffer[start + 4] = readLongBE4(16);
            buffer[start + 5] = readLongBE4(20);
            buffer[start + 6] = readLongBE4(24);
            buffer[start + 7] = readLongBE4(28);
            break;
        case 5:
            buffer[start + 0] = readLongBE5(0);
            buffer[start + 1] = readLongBE5(5);
            buffer[start + 2] = readLongBE5(10);
            buffer[start + 3] = readLongBE5(15);
            buffer[start + 4] = readLongBE5(20);
            buffer[start + 5] = readLongBE5(25);
            buffer[start + 6] = readLongBE5(30);
            buffer[start + 7] = readLongBE5(35);
            break;
        case 6:
            buffer[start + 0] = readLongBE6(0);
            buffer[start + 1] = readLongBE6(6);
            buffer[start + 2] = readLongBE6(12);
            buffer[start + 3] = readLongBE6(18);
            buffer[start + 4] = readLongBE6(24);
            buffer[start + 5] = readLongBE6(30);
            buffer[start + 6] = readLongBE6(36);
            buffer[start + 7] = readLongBE6(42);
            break;
        case 7:
            buffer[start + 0] = readLongBE7(0);
            buffer[start + 1] = readLongBE7(7);
            buffer[start + 2] = readLongBE7(14);
            buffer[start + 3] = readLongBE7(21);
            buffer[start + 4] = readLongBE7(28);
            buffer[start + 5] = readLongBE7(35);
            buffer[start + 6] = readLongBE7(42);
            buffer[start + 7] = readLongBE7(49);
            break;
        case 8:
            buffer[start + 0] = readLongBE8(0);
            buffer[start + 1] = readLongBE8(8);
            buffer[start + 2] = readLongBE8(16);
            buffer[start + 3] = readLongBE8(24);
            buffer[start + 4] = readLongBE8(32);
            buffer[start + 5] = readLongBE8(40);
            buffer[start + 6] = readLongBE8(48);
            buffer[start + 7] = readLongBE8(56);
            break;
        default:
            break;
    }
}


void EncodingUtils::readRemainingLongs(const std::shared_ptr<ByteBuffer> &input, long *buffer,
                                       int offset, int remainder, int numBytes) {
    int toRead = remainder * numBytes;
    // bulk read to buffer
    int bytesRead = input->read(readBuffer, 0, toRead);
    while (bytesRead != toRead) {
        bytesRead += input->read(readBuffer, bytesRead, toRead - bytesRead);
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
                buffer[offset++] = readLongBE2(idx * 2);
                remainder--;
                idx++;
            }
            break;
        case 3:
            while (remainder > 0) {
                buffer[offset++] = readLongBE3(idx * 3);
                remainder--;
                idx++;
            }
            break;
        case 4:
            while (remainder > 0) {
                buffer[offset++] = readLongBE4(idx * 4);
                remainder--;
                idx++;
            }
            break;
        case 5:
            while (remainder > 0) {
                buffer[offset++] = readLongBE5(idx * 5);
                remainder--;
                idx++;
            }
            break;
        case 6:
            while (remainder > 0) {
                buffer[offset++] = readLongBE6(idx * 6);
                remainder--;
                idx++;
            }
            break;
        case 7:
            while (remainder > 0) {
                buffer[offset++] = readLongBE7(idx * 7);
                remainder--;
                idx++;
            }
            break;
        case 8:
            while (remainder > 0) {
                buffer[offset++] = readLongBE8(idx * 8);
                remainder--;
                idx++;
            }
            break;
        default:
            break;
    }
}


long EncodingUtils::readLongBE2(int rbOffset) {
    return (((readBuffer[rbOffset] & 255) << 8)
            + ((readBuffer[rbOffset + 1] & 255) << 0));
}
long EncodingUtils::readLongBE3(int rbOffset) {
    return (((readBuffer[rbOffset] & 255) << 16)
            + ((readBuffer[rbOffset + 1] & 255) << 8)
            + ((readBuffer[rbOffset + 2] & 255) << 0));
}

long EncodingUtils::readLongBE4(int rbOffset) {
    return (((long) (readBuffer[rbOffset] & 255) << 24)
            + ((readBuffer[rbOffset + 1] & 255) << 16)
            + ((readBuffer[rbOffset + 2] & 255) << 8)
            + ((readBuffer[rbOffset + 3] & 255) << 0));
}

long EncodingUtils::readLongBE5(int rbOffset) {
    return (((long) (readBuffer[rbOffset] & 255) << 32)
            + ((long) (readBuffer[rbOffset + 1] & 255) << 24)
            + ((readBuffer[rbOffset + 2] & 255) << 16)
            + ((readBuffer[rbOffset + 3] & 255) << 8)
            + ((readBuffer[rbOffset + 4] & 255) << 0));
}

long EncodingUtils::readLongBE6(int rbOffset) {
    return (((long) (readBuffer[rbOffset] & 255) << 40)
            + ((long) (readBuffer[rbOffset + 1] & 255) << 32)
            + ((long) (readBuffer[rbOffset + 2] & 255) << 24)
            + ((readBuffer[rbOffset + 3] & 255) << 16)
            + ((readBuffer[rbOffset + 4] & 255) << 8)
            + ((readBuffer[rbOffset + 5] & 255) << 0));
}

long EncodingUtils::readLongBE7(int rbOffset) {
    return (((long) (readBuffer[rbOffset] & 255) << 48)
            + ((long) (readBuffer[rbOffset + 1] & 255) << 40)
            + ((long) (readBuffer[rbOffset + 2] & 255) << 32)
            + ((long) (readBuffer[rbOffset + 3] & 255) << 24)
            + ((readBuffer[rbOffset + 4] & 255) << 16)
            + ((readBuffer[rbOffset + 5] & 255) << 8)
            + ((readBuffer[rbOffset + 6] & 255) << 0));
}

long EncodingUtils::readLongBE8(int rbOffset) {
    return (((long) (readBuffer[rbOffset] & 255) << 56)
            + ((long) (readBuffer[rbOffset + 1] & 255) << 48)
            + ((long) (readBuffer[rbOffset + 2] & 255) << 40)
            + ((long) (readBuffer[rbOffset + 3] & 255) << 32)
            + ((long) (readBuffer[rbOffset + 4] & 255) << 24)
            + ((readBuffer[rbOffset + 5] & 255) << 16)
            + ((readBuffer[rbOffset + 6] & 255) << 8)
            + ((readBuffer[rbOffset + 7] & 255) << 0));
}

EncodingUtils::~EncodingUtils() {
	if(readBuffer != nullptr) {
        delete [] readBuffer;
        readBuffer = nullptr;
	}
	if(writeBuffer != nullptr) {
		delete [] writeBuffer;
		writeBuffer = nullptr;
	}
}

// -----------------------------------------------------------
/**
 * @author khaiwang
 * @date 8/10/2023
 * @brief encodingUtils encoding part, modified based on java version
 */

// count
int EncodingUtils::encodeBitWidth(int n) {
    n = getClosestFixedBits(n);

    if (n >= 1 && n <= 24)
    {
        return n - 1;
    }
    else if (n > 24 && n <= 26)
    {
        return FixedBitSizes::TWENTYSIX;
    }
    else if (n > 26 && n <= 28)
    {
        return FixedBitSizes::TWENTYEIGHT;
    }
    else if (n > 28 && n <= 30)
    {
        return FixedBitSizes::THIRTY;
    }
    else if (n > 30 && n <= 32)
    {
        return FixedBitSizes::THIRTYTWO;
    }
    else if (n > 32 && n <= 40)
    {
        return FixedBitSizes::FORTY;
    }
    else if (n > 40 && n <= 48)
    {
        return FixedBitSizes::FORTYEIGHT;
    }
    else if (n > 48 && n <= 56)
    {
        return FixedBitSizes::FIFTYSIX;
    }
    else
    {
        return FixedBitSizes::SIXTYFOUR;
    }
}

int EncodingUtils::getClosestFixedBits(int n) {
    if (n == 0) {
        return 1;
    }

    if (n >= 1 && n <= 24) {
        return n;
    }
    else if (n > 24 && n <= 26) {
        return 26;
    }
    else if (n > 26 && n <= 28) {
        return 28;
    }
    else if (n > 28 && n <= 30) {
        return 30;
    }
    else if (n > 30 && n <= 32) {
        return 32;
    }
    else if (n > 32 && n <= 40) {
        return 40;
    }
    else if (n > 40 && n <= 48) {
        return 48;
    }
    else if (n > 48 && n <= 56) {
        return 56;
    }
    else {
        return 64;
    }
}

void EncodingUtils::unrolledBitPack1(long* input, int offset, int len, 
                                     std::shared_ptr<ByteBuffer> output) {
    int numHops = 8;
    int remainder = len % numHops;
    int endOffset = offset + len;
    int endUnroll = endOffset - remainder;
    byte val = 0;
    for(int i = offset; i < endUnroll; i += numHops) {
        val = (byte) (val | ((input[i] & 1) << 7)
                    | ((input[i + 1] & 1) << 6)
                    | ((input[i + 2] & 1) << 5)
                    | ((input[i + 3] & 1) << 4)
                    | ((input[i + 4] & 1) << 3)
                    | ((input[i + 5] & 1) << 2)
                    | ((input[i + 6] & 1) << 1)
                    | (input[i + 7]) & 1);
        output->put(val);
        val = 0;
    }

    if(remainder > 0) {
        int startShift = 7;
        for(int i = endUnroll; i < endOffset; ++i) {
            val = (byte) (val | (input[i] & 1) << startShift);
            startShift--;
        }
        output->put(val);
    }
}

void EncodingUtils::unrolledBitPack2(long* input, int offset, int len, 
                                     std::shared_ptr<ByteBuffer> output) {
    int numHops = 4;
    int remainder = len % numHops;
    int endOffset = offset + len;
    int endUnroll = endOffset - remainder;
    byte val = 0;
    for(int i = offset; i < endUnroll; i += numHops) {
        val = (byte) (val | ((input[i] & 3) << 6)
                    | ((input[i + 1] & 3) << 4)
                    | ((input[i + 2] & 3) << 2)
                    | ((input[i + 3] & 3)));
        output->put(val);
        val = 0;
    }

    if(remainder > 0) {
        int startShift = 6;
        for(int i = endUnroll; i < endOffset; ++i) {
            val = (int) (val | (input[i] & 3) << startShift);
            startShift -= 2;
        }
        output->put(val);
    }
}

void EncodingUtils::unrolledBitPack4(long* input, int offset, int len, 
                                     std::shared_ptr<ByteBuffer> output) {
    int numHops = 2;
    int remainder = len % numHops;
    int endOffset = offset + len;
    int endUnroll = endOffset - remainder;
    byte val = 0;
    for(int i = offset; i < endUnroll; i += numHops) {
        val = (byte) (val | ((input[i] & 15) << 4)
                    | (input[i + 1] & 15));
        output->put(val);
        val = 0;
    }

    if(remainder > 0) {
        int startShift = 4;
        for(int i = endUnroll; i < endOffset; ++i) {
            val = (byte) (val | (input[i] & 15) << startShift);
            startShift -= 4;
        }
        output->put(val);
    }
}

void EncodingUtils::unrolledBitPack8(long* input, int offset, int len, std::shared_ptr<ByteBuffer> output) {
    unrolledBitPackBytes(input, offset, len, output, 1);
}

void EncodingUtils::unrolledBitPack16(long* input, int offset, int len, 
                                      std::shared_ptr<ByteBuffer> output) {
    unrolledBitPackBytes(input, offset, len, output, 2);
}   

void EncodingUtils::unrolledBitPack24(long* input, int offset, int len, 
                                      std::shared_ptr<ByteBuffer> output) {
    unrolledBitPackBytes(input, offset, len, output, 3);
}

void EncodingUtils::unrolledBitPack32(long* input, int offset, int len, 
                                      std::shared_ptr<ByteBuffer> output) {
    unrolledBitPackBytes(input, offset, len, output, 4);
}

void EncodingUtils::unrolledBitPack40(long* input, int offset, int len, 
                                      std::shared_ptr<ByteBuffer> output) {
    unrolledBitPackBytes(input, offset, len, output, 5);
}

void EncodingUtils::unrolledBitPack48(long* input, int offset, int len, 
                                      std::shared_ptr<ByteBuffer> output) {
    unrolledBitPackBytes(input, offset, len, output, 6);
}

void EncodingUtils::unrolledBitPack56(long* input, int offset, int len, 
                                      std::shared_ptr<ByteBuffer> output) {
    unrolledBitPackBytes(input, offset, len, output, 7);
}

void EncodingUtils::unrolledBitPack64(long* input, int offset, int len, 
                                      std::shared_ptr<ByteBuffer> output) {
    unrolledBitPackBytes(input, offset, len, output, 8);
}

void EncodingUtils::unrolledBitPackBytes(long* input, int offset, int len, 
                                         std::shared_ptr<ByteBuffer> output, int numBytes) {
    int numHops = 8;
    int remainder = len % numHops;
    int endOffset = offset + len;
    int endUnroll = endOffset - remainder;
    int i = offset;
    for(; i < endUnroll; i += numHops) {
        // PENDING: in C++ version, we will change data into little endian
        writeLongBE(output, input, i, numHops, numBytes);
    }

    if(remainder > 0) {
        writeRemainingLongs(output, i, input, remainder, numBytes);
    }
}

void EncodingUtils::writeIntLE(std::shared_ptr<ByteBuffer> output, int value)
{
    writeBuffer[0] = (byte) ((value) & 0xff);
    writeBuffer[1] = (byte) ((value >> 8) & 0xff);
    writeBuffer[2] = (byte) ((value >> 16) & 0xff);
    writeBuffer[3] = (byte) ((value >> 24) & 0xff);
    output->putBytes(writeBuffer, 4);
}

void EncodingUtils::writeLongLE(std::shared_ptr<ByteBuffer> output, long value)
{
    writeBuffer[0] = (byte) ((value) & 0xff);
    writeBuffer[1] = (byte) ((value >> 8) & 0xff);
    writeBuffer[2] = (byte) ((value >> 16) & 0xff);
    writeBuffer[3] = (byte) ((value >> 24) & 0xff);
    writeBuffer[4] = (byte) ((value >> 32) & 0xff);
    writeBuffer[5] = (byte) ((value >> 40) & 0xff);
    writeBuffer[6] = (byte) ((value >> 48) & 0xff);
    writeBuffer[7] = (byte) ((value >> 56) & 0xff);
    output->putBytes(writeBuffer, 8);
}

void EncodingUtils::writeIntBE(std::shared_ptr<ByteBuffer> output, int value)
{
    writeBuffer[3] = (byte) ((value) & 0xff);
    writeBuffer[2] = (byte) ((value >> 8) & 0xff);
    writeBuffer[1] = (byte) ((value >> 16) & 0xff);
    writeBuffer[0] = (byte) ((value >> 24) & 0xff);
    output->putBytes(writeBuffer, 4);
}

void EncodingUtils::writeLongBE(std::shared_ptr<ByteBuffer> output, long value)
{
    writeBuffer[7] = (byte) ((value) & 0xff);
    writeBuffer[6] = (byte) ((value >> 8) & 0xff);
    writeBuffer[5] = (byte) ((value >> 16) & 0xff);
    writeBuffer[4] = (byte) ((value >> 24) & 0xff);
    writeBuffer[3] = (byte) ((value >> 32) & 0xff);
    writeBuffer[2] = (byte) ((value >> 40) & 0xff);
    writeBuffer[1] = (byte) ((value >> 48) & 0xff);
    writeBuffer[0] = (byte) ((value >> 56) & 0xff);
    output->putBytes(writeBuffer, 8);
}

/**
 * @brief write a encoded long value into the output in little endian
*/
void EncodingUtils::writeLongBE(std::shared_ptr<ByteBuffer> output, 
                                long* input, int offset, int numHops, int numBytes) {
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
    int toWrite = numHops * numBytes;
    output->putBytes(writeBuffer, toWrite);
}

void EncodingUtils::writeLongBE2(std::shared_ptr<ByteBuffer> output, 
                                 long val, int wbOffset) {
    writeBuffer[wbOffset] = (byte) (((unsigned long)val) >> 8);
    writeBuffer[wbOffset + 1] = (byte) (val);
}

void EncodingUtils::writeLongBE3(std::shared_ptr<ByteBuffer> output, 
                                 long val, int wbOffset) {
    writeBuffer[wbOffset] = (byte) (((unsigned long)val) >> 16);
    writeBuffer[wbOffset + 1] = (byte) (((unsigned long)val) >> 8);
    writeBuffer[wbOffset + 2] = (byte) (val);
}

void EncodingUtils::writeLongBE4(std::shared_ptr<ByteBuffer> output, 
                                 long val, int wbOffset) {
    writeBuffer[wbOffset] = (byte) (((unsigned long)val) >> 24);
    writeBuffer[wbOffset + 1] = (byte) (((unsigned long)val) >> 16);
    writeBuffer[wbOffset + 2] = (byte) (((unsigned long)val) >> 8);
    writeBuffer[wbOffset + 3] = (byte) (val);
}

void EncodingUtils::writeLongBE5(std::shared_ptr<ByteBuffer> output, 
                                 long val, int wbOffset) {
    writeBuffer[wbOffset] = (byte) (((unsigned long)val) >> 32);
    writeBuffer[wbOffset + 1] = (byte) (((unsigned long)val) >> 24);
    writeBuffer[wbOffset + 2] = (byte) (((unsigned long)val) >> 16);
    writeBuffer[wbOffset + 3] = (byte) (((unsigned long)val) >> 8);
    writeBuffer[wbOffset + 4] = (byte) (val);
}

void EncodingUtils::writeLongBE6(std::shared_ptr<ByteBuffer> output, 
                                 long val, int wbOffset) {
    writeBuffer[wbOffset] = (byte) (((unsigned long)val) >> 40);
    writeBuffer[wbOffset + 1] = (byte) (((unsigned long)val) >> 32);
    writeBuffer[wbOffset + 2] = (byte) (((unsigned long)val) >> 24);
    writeBuffer[wbOffset + 3] = (byte) (((unsigned long)val) >> 16);
    writeBuffer[wbOffset + 4] = (byte) (((unsigned long)val) >> 8);
    writeBuffer[wbOffset + 5] = (byte) (val);
}

void EncodingUtils::writeLongBE7(std::shared_ptr<ByteBuffer> output, 
                                 long val, int wbOffset) {
    writeBuffer[wbOffset] = (byte) (((unsigned long)val) >> 48);
    writeBuffer[wbOffset + 1] = (byte) (((unsigned long)val) >> 40);
    writeBuffer[wbOffset + 2] = (byte) (((unsigned long)val) >> 32);
    writeBuffer[wbOffset + 3] = (byte) (((unsigned long)val) >> 24);
    writeBuffer[wbOffset + 4] = (byte) (((unsigned long)val) >> 16);
    writeBuffer[wbOffset + 5] = (byte) (((unsigned long)val) >> 8);
    writeBuffer[wbOffset + 6] = (byte) (val);
}

void EncodingUtils::writeLongBE8(std::shared_ptr<ByteBuffer> output, 
                                 long val, int wbOffset) {
    writeBuffer[wbOffset] = (byte) (((unsigned long)val) >> 56);
    writeBuffer[wbOffset + 1] = (byte) (((unsigned long)val) >> 48);
    writeBuffer[wbOffset + 2] = (byte) (((unsigned long)val) >> 40);
    writeBuffer[wbOffset + 3] = (byte) (((unsigned long)val) >> 32);
    writeBuffer[wbOffset + 4] = (byte) (((unsigned long)val) >> 24);
    writeBuffer[wbOffset + 5] = (byte) (((unsigned long)val) >> 16);
    writeBuffer[wbOffset + 6] = (byte) (((unsigned long)val) >> 8);
    writeBuffer[wbOffset + 7] = (byte) (val);
}

void EncodingUtils::writeRemainingLongs(std::shared_ptr<ByteBuffer> output, int offset, 
                                        long* input, int remainder, int numBytes) {
    int numHops = remainder;
    int idx = 0;
    switch(numBytes) {
        case 1:
            while (remainder > 0)
            {
                writeBuffer[idx] = (byte) (input[offset + idx] & 255);
                remainder--;
                idx++;
            }
            break;
        case 2:
            while (remainder > 0)
            {
                writeLongBE2(output, input[offset + idx], idx * 2);
                remainder--;
                idx++;
            }
            break;
        case 3:
            while (remainder > 0)
            {
                writeLongBE3(output, input[offset + idx], idx * 3);
                remainder--;
                idx++;
            }
            break;
        case 4:
            while (remainder > 0)
            {
                writeLongBE4(output, input[offset + idx], idx * 4);
                remainder--;
                idx++;
            }
            break;
        case 5:
            while (remainder > 0)
            {
                writeLongBE5(output, input[offset + idx], idx * 5);
                remainder--;
                idx++;
            }
            break;
        case 6:
            while (remainder > 0)
            {
                writeLongBE6(output, input[offset + idx], idx * 6);
                remainder--;
                idx++;
            }
            break;
        case 7:
            while (remainder > 0)
            {
                writeLongBE7(output, input[offset + idx], idx * 7);
                remainder--;
                idx++;
            }
            break;
        case 8:
            while (remainder > 0)
            {
                writeLongBE8(output, input[offset + idx], idx * 8);
                remainder--;
                idx++;
            }
            break;
        default:
            break;

    }
    int toWrite = numHops * numBytes;
    output->putBytes(writeBuffer, toWrite);
}
