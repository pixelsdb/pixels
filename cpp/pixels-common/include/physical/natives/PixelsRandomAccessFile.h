//
// Created by yuliangyong on 2023-03-02.
//

#ifndef PIXELS_PIXELSRANDOMACCESSFILE_H
#define PIXELS_PIXELSRANDOMACCESSFILE_H

#include <iostream>
#include "physical/natives/ByteBuffer.h"
class PixelsRandomAccessFile {
public:
    virtual void seek(long off) = 0;
    virtual long length() = 0;
    virtual std::shared_ptr<ByteBuffer> readFully(int len) = 0;
	virtual std::shared_ptr<ByteBuffer> readFully(int len, std::shared_ptr<ByteBuffer> bb) = 0;
    virtual void close() = 0;
    virtual long readLong() = 0;
    virtual char readChar() = 0;
    virtual int readInt() = 0;
//    virtual std::string readLine();
//    virtual std::string readUTF();
};

#endif //PIXELS_PIXELSRANDOMACCESSFILE_H
