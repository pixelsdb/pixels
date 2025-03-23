//
// Created by yuly on 19.04.23.
//
#include "physical/natives/DirectIoLib.h"


DirectIoLib::DirectIoLib(int fsBlockSize) {
	this->fsBlockSize = fsBlockSize;
	this->fsBlockNotMask = ~((long) fsBlockSize - 1);
}

std::shared_ptr<ByteBuffer> DirectIoLib::allocateDirectBuffer(long size) {
	int toAllocate = blockEnd(size) + (size == 1? 0: fsBlockSize);
	uint8_t * directBufferPointer;
	posix_memalign((void **)&directBufferPointer, fsBlockSize, toAllocate);
	auto directBuffer = std::make_shared<ByteBuffer>(directBufferPointer, toAllocate, false);
	return directBuffer;
}

std::shared_ptr<ByteBuffer> DirectIoLib::read(int fd, long fileOffset,
                                              std::shared_ptr<ByteBuffer> directBuffer, long length) {
	// the file will be read from blockStart(fileOffset), and the first fileDelta bytes should be ignored.
	long fileOffsetAligned = blockStart(fileOffset);
	long toRead = blockEnd(fileOffset + length) - blockStart(fileOffset);
	if(pread(fd, directBuffer->getPointer(), toRead, fileOffsetAligned) == -1) {
		throw InvalidArgumentException("DirectIoLib::read: pread fail. ");
	}
	auto bb = std::make_shared<ByteBuffer>(*directBuffer,
	                                       fileOffset - fileOffsetAligned, length);
	return bb;
}


long DirectIoLib::blockStart(long value) {
	return (value & fsBlockNotMask);
}

long DirectIoLib::blockEnd(long value) {
	return (value + fsBlockSize - 1) & fsBlockNotMask;
}

