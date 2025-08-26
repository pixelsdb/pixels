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
 * @create 2023-05-25
 */
#include "physical/BufferPool.h"
#include <chrono>
#include <duckdb/storage/buffer/buffer_pool.hpp>
#include <iostream>
#include <physical/natives/DirectUringRandomAccessFile.h>
#include <stdexcept>

using TimePoint = std::chrono::high_resolution_clock::time_point;
using Duration = std::chrono::duration<double, std::milli>;

TimePoint getCurrentTime() {
    return std::chrono::high_resolution_clock::now();
}


// global
std::string columnSizePath = ConfigFactory::Instance().getProperty("pixel.column.size.path");
std::shared_ptr <ColumnSizeCSVReader> csvReader;
int sliceSize= std::stoi(ConfigFactory::Instance().getProperty("pixel.bufferpool.sliceSize"));
int bufferSize=std::stoi(ConfigFactory::Instance().getProperty("pixel.bufferpool.bufferpoolSize"));
const size_t SLICE_SIZE = sliceSize * 1024;
int bufferNum=std::stoi(ConfigFactory::Instance().getProperty("pixel.bufferpool.bufferNum"));
std::mutex  BufferPool::bufferPoolMutex;

// thread_local
thread_local bool BufferPool::isInitialized;
thread_local std::vector <std::shared_ptr<BufferPoolEntry>> BufferPool::registeredBuffers[2];
thread_local long BufferPool::global_free_size=0;
thread_local long BufferPool::global_used_size = 0;
thread_local  std::shared_ptr <DirectIoLib> BufferPool::directIoLib;
thread_local int BufferPool::nextRingIndex=1;
thread_local  std::shared_ptr<BufferPoolEntry> BufferPool::nextEmptyBufferPoolEntry[2]={nullptr,nullptr};
thread_local std::vector<BufferPoolEntry> globalBuffers;
thread_local int BufferPool::colCount = 0;
// The currBufferIdx is set to 1. When executing the first file, this value is 0
// since we call switch function first.
thread_local int BufferPool::currBufferIdx = 1;
thread_local int BufferPool::nextBufferIdx = 0;

thread_local std::map<uint32_t, std::shared_ptr<ByteBuffer>> BufferPool::buffersAllocated[2];
thread_local std::unordered_map<uint32_t, std::shared_ptr<BufferPool::BufferPoolManagedEntry>> BufferPool::ringBufferMap[2];
thread_local size_t BufferPool::thread_local_used_size[2]={0,0};
thread_local int BufferPool::thread_local_buffer_count[2]={0,0};

void BufferPool::Initialize(std::vector <uint32_t> colIds, std::vector <uint64_t> bytes,
                            std::vector <std::string> columnNames)
{

    assert(colIds.size() == bytes.size());
    int fsBlockSize = std::stoi(ConfigFactory::Instance().getProperty("localfs.block.size"));
    auto strToBool = [](const std::string& s) {
        return s == "true" || s == "1" || s == "yes";
    };
    std::string configValue = ConfigFactory::Instance().getProperty("pixel.bufferpool.fixedsize");
    bool isFixedSize = strToBool(configValue);

    // give the maximal column size, which is stored in csv reader
    if (!BufferPool::isInitialized)
    {
        currBufferIdx = 0;
        nextBufferIdx = 1;
        directIoLib = std::make_shared<DirectIoLib>(fsBlockSize);
        BufferPool::InitializeBuffers();
        for (int i = 0; i < colIds.size(); i++)
        {
            uint32_t colId = colIds.at(i);
            if (isFixedSize) {
                if (!columnSizePath.empty())
                {
                    csvReader = std::make_shared<ColumnSizeCSVReader>(columnSizePath);
                }
            }
            auto byte=bytes.at(i);
            BufferPool::ringBufferMap[currBufferIdx][colId]=std::make_shared<BufferPoolManagedEntry>(registeredBuffers[currBufferIdx][0],0,byte,0);
            // BufferPool::ringBufferMap[nextBufferIdx][colId]=registeredBuffers[nextBufferIdx][0];
            // std::cout<<"Initialized columnID:"<<colId<<"column name:"<<columnNames[colId]<<" byte: "<<byte<<std::endl;
        }
        BufferPool::colCount = colIds.size();
        BufferPool::isInitialized = true;

    }
    else
    {
        // check if resize the buffer is needed
        // 1.add more columns
        // 2.resive the buffer
        //
        // assert(colIds.size() == BufferPool::colCount);
        for (int i = 0; i < colIds.size(); i++)
        {
            uint32_t colId = colIds.at(i);
            uint64_t byte = bytes.at(i);
            if (BufferPool::ringBufferMap[currBufferIdx].find(colId)==BufferPool::ringBufferMap[currBufferIdx].end())
                BufferPool::ringBufferMap[currBufferIdx][colId]=std::make_shared<BufferPoolManagedEntry>(registeredBuffers[currBufferIdx][0],0,byte,0);
            // BufferPool::ringBufferMap[nextBufferIdx][colId]=registeredBuffers[nextBufferIdx][0];
            // GetBuffer(colId,byte,columnNames.at(i));
            // std::cout<<"Not Initialized columnID:"<<colId<<"column name:"<<columnNames[colId]<<" byte: "<<byte
            // <<"allocated buffer:"<<buffersAllocated[currBufferIdx][colId]->size()<<std::endl;
        }
    }
}


void BufferPool::InitializeBuffers() {
    for (int idx = 0; idx < 2; idx++)
    {
        const int size_=bufferSize+EXTRA_POOL_SIZE;
        // Calculate the required space, allocate it in advance, and then register it.
        std::shared_ptr<BufferPoolEntry> buffer_pool_entry=std::make_shared<BufferPoolEntry>(size_,sliceSize,directIoLib,idx,0);
        registeredBuffers[idx].emplace_back(buffer_pool_entry);
        buffer_pool_entry->setInUse(true);
        global_free_size+=size_;
    }
}


int64_t BufferPool::GetBufferId()
{
    return currBufferIdx;
}

std::shared_ptr<ByteBuffer> BufferPool::AllocateNewBuffer(std::shared_ptr<BufferPoolManagedEntry> currentBufferManagedEntry, uint32_t colId,uint64_t byte,std::string columnName) {
        auto strToBool = [](const std::string& s) {
            return s == "true" || s == "1" || s == "yes";
        };
        std::string configValue = ConfigFactory::Instance().getProperty("pixel.bufferpool.fixedsize");
        bool isFixedSize = strToBool(configValue);
        if (isFixedSize) {
            byte=csvReader->get(columnName);
            // std::cout<<"colId: "<<colId<<" columnName: "<<columnName<<" byte:"<<byte<<std::endl;
        }

        size_t sliceCount = (byte + SLICE_SIZE - 1) / SLICE_SIZE+1;
        size_t totalSize = (sliceCount+1)* SLICE_SIZE;
        // std::cout<<"currBufindex: "<<currBufferIdx<<" colId:"<<colId<<" get bigger buffer! byte:"<<byte<<std::endl;
        // need to reallocate
        // get origin registered ByteBuffer
        auto currentBuffer=currentBufferManagedEntry->getBufferPoolEntry();
        std::shared_ptr<ByteBuffer> original = currentBuffer->getBuffer();

        // get offset
        size_t offset = currentBuffer->getNextFreeIndex();
        // fisrt find anthor empty buffer

        if (offset + totalSize > original->size())
        {

            if (nextEmptyBufferPoolEntry[currBufferIdx]!=nullptr&&
                nextEmptyBufferPoolEntry[currBufferIdx]->getNextFreeIndex()+totalSize<nextEmptyBufferPoolEntry[currBufferIdx]->getSize()) {
            // there are anthor regisitered Buffers
                // std::cout<<"curBufID:"<<currBufferIdx<<" colID:"<<colId<<" "<<columnName<<" has registered"<<std::endl;
                currentBuffer=nextEmptyBufferPoolEntry[currBufferIdx];
            }else {
                // find more space
                // 1. register a new io_uring bounded buffer
                // 2. reallocate current buffer
                // throw std::runtime_error("Not enough space in the buffer");
                // std::cout<<" offset:"<<offset<<" totalSize:"<<totalSize<<" original-size():"<<original->size()<<std::endl;
                currentBuffer->setInUse(false);
                currentBuffer=BufferPool::AddNewBuffer(currentBuffer->getSize());
                std::vector<std::shared_ptr<ByteBuffer>> buffers;
                buffers.emplace_back(currentBuffer->getBuffer());
                if (!::DirectUringRandomAccessFile::RegisterMoreBuffer(currentBuffer->getRingIndex(),buffers)) {
                    throw std::runtime_error("Failed to register more buffers");
                }
                currentBuffer->setInUse(true);
                currentBuffer->setIsRegistered(true);
            }
            offset=currentBuffer->getNextFreeIndex();
            // change the find bitmap
            auto newBufferPoolManageEntry=std::make_shared<BufferPoolManagedEntry>(currentBuffer,currentBuffer->getRingIndex(),totalSize,offset);
            BufferPool::ringBufferMap[currBufferIdx][colId]=newBufferPoolManageEntry;
            newBufferPoolManageEntry->setStatus(BufferPoolManagedEntry::State::AllocatedAndInUse);
            original=currentBuffer->getBuffer();
        }
        // update bitmap (maybe costly)
        // size_t startSlice = offset / SLICE_SIZE;
        // for (size_t i = 0; i < sliceCount; ++i) {
        //     size_t sliceIndex = startSlice + i;
        //     if (currentBuffer->getBitmap()->test(sliceIndex)) {
        //         throw std::runtime_error("Buffer slice already used! Potential bitmap inconsistency.");
        //     }
        //     currentBuffer->getBitmap()->set(sliceIndex);
        // }

        currentBuffer->setNextFreeIndex(offset + totalSize);

        std::shared_ptr<ByteBuffer> sliced = original->slice(offset, totalSize);
        currentBuffer->addCol(colId,sliced->size());
        BufferPool::buffersAllocated[currBufferIdx][colId] = sliced;
        auto newBufferPoolManageEntry=BufferPool::ringBufferMap[currBufferIdx][colId];
        newBufferPoolManageEntry->setStatus(BufferPoolManagedEntry::State::AllocatedAndInUse);
        newBufferPoolManageEntry->setCurrentSize(sliced->size());
        newBufferPoolManageEntry->setOffset(offset);
        newBufferPoolManageEntry->setRingIndex(currentBuffer->getRingIndex());

        // std::cout<<"buffer pointer:"<<static_cast<void*>(sliced->getPointer())<<
        //     " length:"<<sliced->size()<<" bufferOffsets:"<<currentBuffer->getNextFreeIndex()<<" buffersAllocated:"<<BufferPool::buffersAllocated[currBufferIdx][colId]->size()<<std::endl;
        global_used_size+=totalSize;
        thread_local_used_size[currBufferIdx]+=totalSize;
        thread_local_buffer_count[currBufferIdx]++;
        return sliced;
}


std::shared_ptr<ByteBuffer> BufferPool::ReusePreviousBuffer(std::shared_ptr<BufferPoolManagedEntry> currentBufferManagedEntry,uint32_t colId,uint64_t byte,std::string columnName) {
    auto currentBuffer=currentBufferManagedEntry->getBufferPoolEntry();
    // std::cout<<"resue previous buffer size:"<<buffersAllocated[currBufferIdx][colId]->size()
    // <<" colID:"<<colId<<" columnName:"<<columnName<<
    //     " currentBufferManagedEntry->getCurrentSize():"<<currentBufferManagedEntry->getCurrentSize()<<" byte:"<<byte<<std::endl;
    return buffersAllocated[currBufferIdx][colId];
}

std::shared_ptr <ByteBuffer> BufferPool::GetBuffer(uint32_t colId,uint64_t byte,std::string columnName)
{
    // another buffer
    // std::lock_guard<std::mutex> lock(bufferPoolMutex);
    auto currentBufferManagedEntry = ringBufferMap[currBufferIdx][colId];
    // 分配情况
    // 情况一 未分配 检查entry的状态
    // 情况二 已经分配
    // 大小情况
    // 情况一 可以复用之前的buffer resuePrevious()
    // 情况二 可以在同一个大buffer中划分出需要的空间 allocateBuffer
    // 情况三 当前大buffer剩余内存不够用了 需要新的大buffer
    if (currentBufferManagedEntry->getStatus()==BufferPoolManagedEntry::State::InitizaledNotAllocated) {
        //未分配
        return AllocateNewBuffer(currentBufferManagedEntry,colId,byte,columnName);
    }
    else {
        // 已经分配
        if (currentBufferManagedEntry->getCurrentSize()>=byte&&currentBufferManagedEntry->getCurrentSize()-directIoLib->getBlockSize()>=byte) {
            // 复用之前的buffer
            return ReusePreviousBuffer(currentBufferManagedEntry,colId,byte,columnName);
        }else {
            return AllocateNewBuffer(currentBufferManagedEntry,colId,byte,columnName);
        }
    }
}

void BufferPool::Reset()
{
    // BufferPool::isInitialized = false;
    // std::lock_guard<std::mutex> lock(bufferPoolMutex);
    for (int idx = 0; idx < 2; idx++)
    {
        // BufferPool::currentBuffers[idx]->clear();;
        BufferPool::buffersAllocated[idx].clear();
        BufferPool::ringBufferMap[idx].clear();
        // BufferPool::registeredBuffers[idx].clear();
        // thread_local_used_size[idx]=0;
        // thread_local_buffer_count[idx]=0;
        // global_free_size=0;
        global_used_size=0;
        for (auto bufferEntry: registeredBuffers[idx]) {
            bufferEntry->reset();
        }
    }




    BufferPool::colCount = 0;
}

void BufferPool::Switch()
{
    currBufferIdx = 1 - currBufferIdx;
    nextBufferIdx = 1 - nextBufferIdx;
}



std::shared_ptr<BufferPoolEntry> BufferPool::AddNewBuffer(size_t size) {
    // std::cout<<"Adding new buffer"<<std::endl;
    if (csvReader!=nullptr) {
        assert(false && "Unexpected code path reached!");
        throw std::logic_error("Unexpected code path reached");
    }
    // Calculate the required space, allocate it in advance, and then register it.
    std::shared_ptr<BufferPoolEntry> buffer_pool_entry=std::make_shared<BufferPoolEntry>(size,sliceSize,directIoLib,currBufferIdx,nextRingIndex++);
    std::cout<<"申请新buffer:"<<nextRingIndex<<std::endl;
    registeredBuffers[currBufferIdx].emplace_back(buffer_pool_entry);
    buffer_pool_entry->setInUse(true);
    global_free_size+=size;
    nextEmptyBufferPoolEntry[currBufferIdx]=buffer_pool_entry;
    return buffer_pool_entry;
}

int BufferPool::getRingIndex(uint32_t colId) {
    return ringBufferMap[currBufferIdx][colId]->getBufferPoolEntry()->getRingIndex();
}

