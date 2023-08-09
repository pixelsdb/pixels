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
    for (int i = offset; i < endUnroll; i = i + numHops)
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
