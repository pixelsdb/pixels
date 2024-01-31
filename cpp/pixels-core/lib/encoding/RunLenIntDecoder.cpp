//
// Created by liyu on 3/20/23.
//

#include "encoding/RunLenIntDecoder.h"

RunLenIntDecoder::RunLenIntDecoder(const std::shared_ptr <ByteBuffer>& bb, bool isSigned) {
    literals = new long[Constants::MAX_SCOPE];
    inputStream = bb;
    this->isSigned = isSigned;
    numLiterals = 0;
    used = 0;
	isRepeating = false;
}

void RunLenIntDecoder::close() {

}

RunLenIntDecoder::~RunLenIntDecoder() {
	if(literals != nullptr) {
		delete[] literals;
		literals = nullptr;
	}
}

long RunLenIntDecoder::next() {
    long result;
    if(used == numLiterals) {
        numLiterals = 0;
        used = 0;
        readValues();
    }
    result = literals[used++];
    return result;
}

void RunLenIntDecoder::readValues() {
	// read the first 2 bits and determine the encoding type
	isRepeating = false;
    int firstByte = (int) inputStream->get();
    if(firstByte < 0) {
        // TODO: logger.error
        used = 0;
        numLiterals = 0;
        return;
    }
    // here we do unsigned shift
    auto currentEncoding = (EncodingType) ((firstByte >> 6) & 0x03);
    switch (currentEncoding) {
        case RunLenIntEncoder::SHORT_REPEAT:
	    	readShortRepeatValues(firstByte);
	        break;
        case RunLenIntEncoder::DIRECT:
            readDirectValues(firstByte);
            break;
        case RunLenIntEncoder::PATCHED_BASE:
            throw InvalidArgumentException("Currently "
                                           "we don't support PATCHED_BASE encoding.");
        case RunLenIntEncoder::DELTA:
		    readDeltaValues(firstByte);
		    break;
        default:
            throw InvalidArgumentException("Not supported encoding type.");
    }
}

void RunLenIntDecoder::readDirectValues(int firstByte) {
    // extract the number of fixed bits;
    uint8_t fbo = (firstByte >> 1) & 0x1f;
    int fb = encodingUtils.decodeBitWidth(fbo);

    // extract run length
    int len = (firstByte & 0x01) << 8;
    len |= inputStream->get();
    // runs are one off
    len += 1;

    // write the unpacked value and zigzag decode to result buffer
    readInts(literals, numLiterals, len, fb, inputStream);
    if(isSigned) {
        for (int i = 0; i < len; i++) {
            literals[numLiterals] = zigzagDecode(literals[numLiterals]);
            numLiterals++;
        }
    } else {
        numLiterals += len;
    }
}

long RunLenIntDecoder::zigzagDecode(long val) {
    return (long) (((uint64_t)val >> 1) ^ -(val & 1));
}

/**
 * Read bitpacked integers from input stream
 */
void RunLenIntDecoder::readInts(long *buffer, int offset, int len, int bitSize,
                           const std::shared_ptr<ByteBuffer> &input) {
    int bitsLeft = 0;
    int current = 0;
    switch (bitSize) {
	    case 1:
            encodingUtils.unrolledUnPack1(buffer, offset, len, input);
            return;
	    case 2:
			encodingUtils.unrolledUnPack2(buffer, offset, len, input);
		    return;
        case 4:
            encodingUtils.unrolledUnPack4(buffer, offset, len, input);
            return;
        case 8:
            encodingUtils.unrolledUnPack8(buffer, offset, len, input);
            return;
	    case 16:
		    encodingUtils.unrolledUnPack16(buffer, offset, len, input);
		    return;
	    case 24:
		    encodingUtils.unrolledUnPack24(buffer, offset, len, input);
		    return;
	    case 32:
		    encodingUtils.unrolledUnPack32(buffer, offset, len, input);
		    return;
	    case 40:
		    encodingUtils.unrolledUnPack40(buffer, offset, len, input);
		    return;
	    case 48:
		    encodingUtils.unrolledUnPack48(buffer, offset, len, input);
		    return;
	    case 56:
		    encodingUtils.unrolledUnPack56(buffer, offset, len, input);
		    return;
	    case 64:
		    encodingUtils.unrolledUnPack64(buffer, offset, len, input);
		    return;
        default:
            throw InvalidArgumentException("RunLenIntDecoder::readInts: "
                                           "not supported bitSize.");
    }
    // TODO: if notthe case, we should write the following code.
}

void RunLenIntDecoder::readDeltaValues(int firstByte) {
	// extract the number of fixed bits;
	uint8_t fb = (((uint32_t)firstByte) >> 1) & 0x1f;
	if(fb != 0) {
		fb = encodingUtils.decodeBitWidth(fb);
	}


	// extract the blob run length
	int len = (firstByte & 0x01) << 8;
	len |= inputStream->get();

	// read the first value stored as vint
	long firstVal = 0;
	if (isSigned) {
		firstVal = readVslong(inputStream);
	}
	else {
		firstVal = readVulong(inputStream);
	}

	// store first value to result buffer
	long prevVal = firstVal;
	literals[numLiterals++] = firstVal;

	// if fixed bits is 0 then all values have fixed delta
	if (fb == 0) {
		// read the fixed delta value stored as vint (deltas
		// can be negative even if all number are positive)
		long fd = readVslong(inputStream);
		if (fd == 0) {
			isRepeating = true;
			assert(numLiterals == 1);
			for(int i = 0; i < len; i++) {
				literals[numLiterals + i] = literals[0];
			}
			numLiterals += len;
		}
		else {
			// add fixed deltas to adjacent values
			for (int i = 0; i < len; i++) {
				literals[numLiterals] = literals[numLiterals - 1] + fd;
				numLiterals++;
			}
		}
	}
	else {
		long deltaBase = readVslong(inputStream);
		// add delta base and first value
		literals[numLiterals++] = firstVal + deltaBase;
		prevVal = literals[numLiterals - 1];
		len -= 1;

		// write the unpacked values, add it to previous values and store final
		// value to result buffer. if the delta base value is negative then it
		// is a decreasing sequence else an increasing sequence
		readInts(literals, numLiterals, len, fb, inputStream);
		while (len > 0) {
			if (deltaBase < 0) {
				literals[numLiterals] = prevVal - literals[numLiterals];
			}
			else {
				literals[numLiterals] = prevVal + literals[numLiterals];
			}
			prevVal = literals[numLiterals];
			len--;
			numLiterals++;
		}
	}

}

/**
     * Read an unsigned long from the input stream, using little endian.
     * @param in the input stream.
     * @return the long value.
     * @throws IOException if an I/O error occurs.
 */
long RunLenIntDecoder::readVulong(const std::shared_ptr<ByteBuffer> &input) {
	long result = 0;
	long b;
	int offset = 0;
	do {
		b = input->get();
		if(b == -1) {
			throw InvalidArgumentException("Reading Vulong past EOF");
		}
		result |= (0x7f & b) << offset;
		offset += 7;
	} while(b >= 0x80);
	return result;
}

/**
     * Read a signed long from the input stream, using little endian.
     * @param in the input stream.
     * @return the long value.
     * @throws IOException if an I/O error occurs.
 */
long RunLenIntDecoder::readVslong(const std::shared_ptr<ByteBuffer> &input) {
	long result = readVulong(input);
	return (((uint64_t)result) >> 1) ^ -(result & 1);
}

void RunLenIntDecoder::readShortRepeatValues(int firstByte) {
	// read the number of bytes occupied by the value
	int size = (((uint32_t)firstByte) >> 3) & 0x07;
	// number of bytes are one off
	size += 1;

	// read the run length
	int len = firstByte & 0x07;
	// run length values are stored only after MIN_REPEAT value is met
	len += Constants::MIN_REPEAT;

	// read the repeated value which is stored using fixed bytes
	long val = bytesToLongBE(inputStream, size);

	if (isSigned) {
		val = zigzagDecode(val);
	}

	if (numLiterals != 0) {
		// currently this always holds, which makes peekNextAvailLength simpler.
		// if this changes, peekNextAvailLength should be adjusted accordingly.
		throw InvalidArgumentException("numLiterals is not zero");
	}

	// repeat the value for length times
	isRepeating = true;
	// TODO: this is not so useful and V1 reader doesn't do that. Fix? Same if delta == 0
	for (int i = 0; i < len; i++) {
		literals[i] = val;
	}
	numLiterals = len;
}

/**
     * Read n bytes from the input stream, and parse them into long value using big endian.
     * @param input the input stream.
     * @param n n bytes.
     * @return the long value.
     * @throws IOException if an I/O error occurs.
 */
long RunLenIntDecoder::bytesToLongBE(const std::shared_ptr<ByteBuffer> &input, int n) {
	long out = 0;
	long val = 0;
	while (n > 0) {
		n--;
		// store it in a long and then shift else integer overflow will occur
		val = input->get();
		out |= (val << (n * 8));
	}
	return out;
}
bool RunLenIntDecoder::hasNext() {
	// print used and inputstream size and pos
	return used != numLiterals || (inputStream->size() - inputStream->getReadPos()) > 0;
}
