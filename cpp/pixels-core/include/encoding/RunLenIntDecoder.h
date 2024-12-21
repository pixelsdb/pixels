/*
 * Copyright 2023 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */

/*
 * @author liyu
 * @create 2023-03-20
 */
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
