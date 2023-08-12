//
// Created by liyu on 3/21/23.
//

#ifndef PIXELS_ENCODINGUTILS_H
#define PIXELS_ENCODINGUTILS_H

#include <memory>
#include "physical/natives/ByteBuffer.h"

class EncodingUtils {
public:
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
