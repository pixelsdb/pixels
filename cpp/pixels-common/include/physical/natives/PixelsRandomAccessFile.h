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
 * @create 2023-03-02
 */
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
