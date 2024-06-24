//
// Created by liyu on 5/25/23.
//

#include "physical/BufferPool.h"

thread_local int BufferPool::colCount = 0;
thread_local std::map<uint32_t, uint64_t> BufferPool::nrBytes;
thread_local bool BufferPool::isInitialized = false;
thread_local std::map<uint32_t, std::shared_ptr<ByteBuffer>> BufferPool::buffers[2];
// The currBufferIdx is set to 1. When executing the first file, this value is 0
// since we call switch function first.
thread_local int BufferPool::currBufferIdx = 1;
thread_local int BufferPool::nextBufferIdx = 0;
std::shared_ptr<DirectIoLib> BufferPool::directIoLib;

void BufferPool::Initialize(std::vector<uint32_t> colIds, std::vector<uint64_t> bytes, std::vector<std::string> columnNames) {
	assert(colIds.size() == bytes.size());
	int fsBlockSize = std::stoi(ConfigFactory::Instance().getProperty("localfs.block.size"));
    std::string columnSizePath = ConfigFactory::Instance().getProperty("pixel.column.size.path");
    std::shared_ptr<ColumnSizeCSVReader> csvReader;
    if (!columnSizePath.empty()) {
        csvReader = std::make_shared<ColumnSizeCSVReader>(columnSizePath);
    }

    // give the maximal column size, which is stored in csv reader
	if(!BufferPool::isInitialized) {
        currBufferIdx = 0;
        nextBufferIdx = 1;
		directIoLib = std::make_shared<DirectIoLib>(fsBlockSize);
		for(int i = 0; i < colIds.size(); i++) {
			uint32_t colId = colIds.at(i);
            std::string columnName = columnNames[colId];
            for(int idx = 0; idx < 2; idx++) {
                std::shared_ptr<ByteBuffer> buffer;
                if (columnSizePath.empty()) {
                    buffer = BufferPool::directIoLib->allocateDirectBuffer(bytes.at(i) + EXTRA_POOL_SIZE);
                } else {
                    buffer = BufferPool::directIoLib->allocateDirectBuffer(csvReader->get(columnName));
                }

                BufferPool::nrBytes[colId] = buffer->size();
                BufferPool::buffers[idx][colId] = buffer;
            }
		}
		BufferPool::colCount = colIds.size();
		BufferPool::isInitialized = true;
	} else {
		// check if resize the buffer is needed
		assert(colIds.size() == BufferPool::colCount);
		for (int i = 0; i < colIds.size(); i++) {
			uint32_t colId = colIds.at(i);
			uint64_t byte = bytes.at(i);
            std::string columnName = columnNames[colId];
			if (BufferPool::nrBytes.find(colId) == BufferPool::nrBytes.end()) {
				throw InvalidArgumentException("BufferPool::Initialize: no such the column id.");
			}
			// Note: this code should never happen in the pixels scenario
			if (BufferPool::nrBytes[colId] < byte) {
				throw InvalidArgumentException("the new buffer byte cannot larger than the previous buffer byte. ");
			}
		}
	}
}

int64_t BufferPool::GetBufferId(uint32_t index) {
    return index + currBufferIdx * colCount;
}

std::shared_ptr<ByteBuffer> BufferPool::GetBuffer(uint32_t colId) {
	return BufferPool::buffers[currBufferIdx][colId];
}

void BufferPool::Reset() {
	BufferPool::isInitialized = false;
	BufferPool::nrBytes.clear();
    for(int idx = 0; idx < 2; idx++) {
        BufferPool::buffers[idx].clear();
    }
	BufferPool::colCount = 0;
}

void BufferPool::Switch() {
    currBufferIdx = 1 - currBufferIdx;
    nextBufferIdx = 1 - nextBufferIdx;
}


