//
// Created by liyu on 3/20/23.
//

#ifndef PIXELS_RUNLENINTDECODER_H
#define PIXELS_RUNLENINTDECODER_H

#include "utils/Constants.h"
#include "encoding/Decoder.h"
#include "encoding/RunLenIntEncoder.h"
#include "exception/InvalidArgumentException.h"
#include "utils/EncodingUtils.h"

typedef RunLenIntEncoder::EncodingType EncodingType;
class RunLenIntDecoder: public Decoder {
public:
    RunLenIntDecoder(const std::shared_ptr<ByteBuffer>& bb, bool isSigned);
    void close() override;
    long next() override;
	bool hasNext() override;
    ~RunLenIntDecoder();
private:

    void readValues();
	void readShortRepeatValues(int firstByte);
    void readDirectValues(int firstByte);
	void readDeltaValues(int firstByte);
	long readVulong(const std::shared_ptr<ByteBuffer>& input);
	long readVslong(const std::shared_ptr<ByteBuffer>& input);
	long bytesToLongBE(const std::shared_ptr<ByteBuffer>& input, int n);
    long zigzagDecode(long val);
    void readInts(long * buffer, int offset, int len, int bitSize,
                  const std::shared_ptr<ByteBuffer>& input);
    long * literals;
    bool isSigned;
    int numLiterals;
    int used;
    std::shared_ptr<ByteBuffer> inputStream;
    EncodingUtils encodingUtils;
	bool isRepeating;
};
#endif //PIXELS_RUNLENINTDECODER_H
