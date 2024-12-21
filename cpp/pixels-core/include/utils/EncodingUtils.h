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
 * @create 2023-03-21
 */
#ifndef PIXELS_ENCODINGUTILS_H
#define PIXELS_ENCODINGUTILS_H

#include <memory>
#include "physical/natives/ByteBuffer.h"

class EncodingUtils {
public:
    using byte = uint8_t;
    EncodingUtils();
	~EncodingUtils();
	int decodeBitWidth(int n);
    void readLongBE(const std::shared_ptr<ByteBuffer> &input, long * buffer,
                    int start, int numHops, int numBytes);
    void readRemainingLongs(const std::shared_ptr<ByteBuffer> &input, long * buffer,
                            int offset, int remainder, int numBytes);
    long readLongBE2(int rbOffset);
    long readLongBE3(int rbOffset);
    long readLongBE4(int rbOffset);
    long readLongBE5(int rbOffset);
    long readLongBE6(int rbOffset);
    long readLongBE7(int rbOffset);
    long readLongBE8(int rbOffset);
    void unrolledUnPackBytes(long *buffer, int offset, int len,
                             const std::shared_ptr<ByteBuffer> &input, int numBytes);
	void unrolledUnPack1(long *buffer, int offset, int len,
	                     const std::shared_ptr<ByteBuffer> &input);
	void unrolledUnPack2(long *buffer, int offset, int len,
	                     const std::shared_ptr<ByteBuffer> &input);
    void unrolledUnPack4(long *buffer, int offset, int len,
                         const std::shared_ptr<ByteBuffer> &input);
    void unrolledUnPack8(long *buffer, int offset, int len,
                         const std::shared_ptr<ByteBuffer> &input);
	void unrolledUnPack16(long *buffer, int offset, int len,
	                      const std::shared_ptr<ByteBuffer> &input);
	void unrolledUnPack24(long *buffer, int offset, int len,
	                      const std::shared_ptr<ByteBuffer> &input);
	void unrolledUnPack32(long *buffer, int offset, int len,
	                      const std::shared_ptr<ByteBuffer> &input);
	void unrolledUnPack40(long *buffer, int offset, int len,
	                      const std::shared_ptr<ByteBuffer> &input);
	void unrolledUnPack48(long *buffer, int offset, int len,
	                      const std::shared_ptr<ByteBuffer> &input);
	void unrolledUnPack56(long *buffer, int offset, int len,
	                      const std::shared_ptr<ByteBuffer> &input);
	void unrolledUnPack64(long *buffer, int offset, int len,
	                      const std::shared_ptr<ByteBuffer> &input);
    // -----------------------------------------------------------
    // encoding utils
    int encodeBitWidth(int n);
    int getClosestFixedBits(int n);
    void unrolledBitPack1(long* input, int offset, int len, 
                          std::shared_ptr<ByteBuffer> output);
    void unrolledBitPack2(long* input, int offset, int len, 
                          std::shared_ptr<ByteBuffer> output);
    void unrolledBitPack4(long* input, int offset, int len, 
                          std::shared_ptr<ByteBuffer> output);
    void unrolledBitPack8(long* input, int offset, int len, 
                          std::shared_ptr<ByteBuffer> output);
    void unrolledBitPack16(long* input, int offset, int len, 
                           std::shared_ptr<ByteBuffer> output);
    void unrolledBitPack24(long* input, int offset, int len, 
                           std::shared_ptr<ByteBuffer> output);
    void unrolledBitPack32(long* input, int offset, int len, 
                           std::shared_ptr<ByteBuffer> output);
    void unrolledBitPack40(long* input, int offset, int len, 
                           std::shared_ptr<ByteBuffer> output);
    void unrolledBitPack48(long* input, int offset, int len, 
                           std::shared_ptr<ByteBuffer> output);                      
    void unrolledBitPack56(long* input, int offset, int len, 
                           std::shared_ptr<ByteBuffer> output);
    void unrolledBitPack64(long* input, int offset, int len, 
                           std::shared_ptr<ByteBuffer> output);
    void unrolledBitPackBytes(long* input, int offset, int len, 
                              std::shared_ptr<ByteBuffer> output, int numBytes);
    void writeIntLE(std::shared_ptr<ByteBuffer> output, int val);
    void writeLongLE(std::shared_ptr<ByteBuffer> output, long val);
    void writeIntBE(std::shared_ptr<ByteBuffer> output, int val);
    void writeLongBE(std::shared_ptr<ByteBuffer> output, long val);
    void writeLongBE(std::shared_ptr<ByteBuffer> output, 
                     long* input, int offset, int numHops, int numBytes);
    void writeLongBE2(std::shared_ptr<ByteBuffer> output, 
                      long val, int wbOffset);
    void writeLongBE3(std::shared_ptr<ByteBuffer> output, 
                      long val, int wbOffset);
    void writeLongBE4(std::shared_ptr<ByteBuffer> output, 
                      long val, int wbOffset);
    void writeLongBE5(std::shared_ptr<ByteBuffer> output, 
                      long val, int wbOffset);
    void writeLongBE6(std::shared_ptr<ByteBuffer> output, 
                      long val, int wbOffset);
    void writeLongBE7(std::shared_ptr<ByteBuffer> output, 
                      long val, int wbOffset);
    void writeLongBE8(std::shared_ptr<ByteBuffer> output, 
                      long val, int wbOffset);     
    void writeRemainingLongs(std::shared_ptr<ByteBuffer> output, int offset, 
                             long* input, int remainder, int numBytes);            
    // -----------------------------------------------------------
private:
enum FixedBitSizes{
        ONE, TWO, THREE, FOUR, FIVE, SIX, SEVEN, EIGHT, NINE, TEN, ELEVEN, TWELVE,
        THIRTEEN, FOURTEEN, FIFTEEN, SIXTEEN, SEVENTEEN, EIGHTEEN, NINETEEN,
        TWENTY, TWENTYONE, TWENTYTWO, TWENTYTHREE, TWENTYFOUR, TWENTYSIX,
        TWENTYEIGHT, THIRTY, THIRTYTWO, FORTY, FORTYEIGHT, FIFTYSIX, SIXTYFOUR
    };
    static int BUFFER_SIZE;
    uint8_t * readBuffer;
    uint8_t * writeBuffer;
};
#endif //PIXELS_ENCODINGUTILS_H
