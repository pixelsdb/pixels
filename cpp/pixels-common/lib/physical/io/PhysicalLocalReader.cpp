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
 * @create 2023-02-27
 */
#include "physical/storage/LocalFS.h"
#include "physical/io/PhysicalLocalReader.h"

#include <utility>
#include "profiler/TimeProfiler.h"

PhysicalLocalReader::PhysicalLocalReader(std::shared_ptr <Storage> storage, std::string path_)
{
    // TODO: should support async
    if (std::dynamic_pointer_cast<LocalFS>(storage).get() != nullptr)
    {
        local = std::dynamic_pointer_cast<LocalFS>(storage);
    }
    else
    {
        throw std::runtime_error("Storage is not LocalFS.");
    }
    if (path_.rfind("file://", 0) != std::string::npos)
    {
        // remove the scheme.
        path_.erase(0, 7);
    }
    path = std::move(path_);
    raf = local->openRaf(path);
    // TODO: get fileid.
    numRequests = 1;
    asyncNumRequests = 0;
}

std::shared_ptr <ByteBuffer> PhysicalLocalReader::readFully(int length)
{
    numRequests++;
    return raf->readFully(length);
}

std::shared_ptr <ByteBuffer> PhysicalLocalReader::readFully(int length, std::shared_ptr <ByteBuffer> bb)
{
    numRequests++;
    return raf->readFully(length, bb);
}

void PhysicalLocalReader::close()
{
    numRequests++;
    raf->close();
}

long PhysicalLocalReader::getFileLength()
{
    numRequests++;
    return raf->length();
}

void PhysicalLocalReader::seek(long desired)
{
    numRequests++;
    raf->seek(desired);
}

long PhysicalLocalReader::readLong()
{
    return raf->readLong();
}

char PhysicalLocalReader::readChar()
{
    return raf->readChar();
}

int PhysicalLocalReader::readInt()
{
    return raf->readInt();
}

std::string PhysicalLocalReader::getName()
{
    if (path.empty())
    {
        return "";
    }
    return path.substr(path.find_last_of('/') + 1);
}
void PhysicalLocalReader::addRingIndex(int ring_index) {
    ring_index_vector.insert(ring_index);
}

std::unordered_set<int>& PhysicalLocalReader::getRingIndexes() {
    return ring_index_vector;
}

std::unordered_map<int,uint32_t> PhysicalLocalReader::getRingIndexCountMap() {
    return ringIndexCountMap;
}

void PhysicalLocalReader::setRingIndexCountMap(std::unordered_map<int,uint32_t> ringIndexCount) {
    //first clear
    ringIndexCountMap.clear();
    ringIndexCountMap=ringIndexCount;
}
std::shared_ptr <ByteBuffer> PhysicalLocalReader::readAsync(int length, std::shared_ptr <ByteBuffer> buffer, int index,int ring_index,int start_offset)
{
    numRequests++;
    if (ConfigFactory::Instance().getProperty("localfs.async.lib") == "iouring")
    {
        auto directRaf = std::static_pointer_cast<DirectUringRandomAccessFile>(raf);
        return directRaf->readAsync(length, std::move(buffer), index,ring_index,start_offset);
    }
    else if (ConfigFactory::Instance().getProperty("localfs.async.lib") == "aio")
    {
        throw InvalidArgumentException("PhysicalLocalReader::readAsync: We don't support aio for our async read yet.");
    }
    else
    {
        throw InvalidArgumentException("PhysicalLocalReader::readAsync: the async read method is unknown. ");
    }

}

void PhysicalLocalReader::readAsyncSubmit(std::unordered_map<int,uint32_t> sizes,std::unordered_set<int> ring_index)
{
    numRequests++;
    if (ConfigFactory::Instance().getProperty("localfs.async.lib") == "iouring")
    {
        auto directRaf = std::static_pointer_cast<DirectUringRandomAccessFile>(raf);
        directRaf->readAsyncSubmit(sizes,ring_index);
    }
    else if (ConfigFactory::Instance().getProperty("localfs.async.lib") == "aio")
    {
        throw InvalidArgumentException("PhysicalLocalReader::readAsync: We don't support aio for our async read yet.");
    }
    else
    {
        throw InvalidArgumentException("PhysicalLocalReader::readAsync: the async read method is unknown. ");
    }
}

void PhysicalLocalReader::readAsyncComplete(std::unordered_map<int,uint32_t> sizes,std::unordered_set<int> ring_index)
{
    numRequests++;
    if (ConfigFactory::Instance().getProperty("localfs.async.lib") == "iouring")
    {
        auto directRaf = std::static_pointer_cast<DirectUringRandomAccessFile>(raf);
        directRaf->readAsyncComplete(sizes,ring_index);
    }
    else if (ConfigFactory::Instance().getProperty("localfs.async.lib") == "aio")
    {
        throw InvalidArgumentException("PhysicalLocalReader::readAsync: We don't support aio for our async read yet.");
    }
    else
    {
        throw InvalidArgumentException("PhysicalLocalReader::readAsync: the async read method is unknown. ");
    }
}


// not use?
void PhysicalLocalReader::readAsyncSubmitAndComplete(uint32_t size,std::unordered_set<int> ring_index)
{
    numRequests++;
    if (ConfigFactory::Instance().getProperty("localfs.async.lib") == "iouring")
    {
        auto directRaf = std::static_pointer_cast<DirectUringRandomAccessFile>(raf);
        std::unordered_map<int,uint32_t> sizes;
        sizes[0]=0;
        directRaf->readAsyncSubmit(sizes,ring_index);
        ::TimeProfiler::Instance().Start("async wait");
        directRaf->readAsyncComplete(sizes,ring_index);
        ::TimeProfiler::Instance().End("async wait");
    }
    else if (ConfigFactory::Instance().getProperty("localfs.async.lib") == "aio")
    {
        throw InvalidArgumentException("PhysicalLocalReader::readAsync: We don't support aio for our async read yet.");
    }
    else
    {
        throw InvalidArgumentException("PhysicalLocalReader::readAsync: the async read method is unknown. ");
    }
}
