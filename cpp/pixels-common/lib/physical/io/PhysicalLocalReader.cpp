//
// Created by liyu on 2/27/23.
//
#include "physical/storage/LocalFS.h"
#include "physical/io/PhysicalLocalReader.h"

#include <utility>
#include "profiler/TimeProfiler.h"
PhysicalLocalReader::PhysicalLocalReader(std::shared_ptr<Storage> storage, std::string path_) {
    // TODO: should support async
    if(std::dynamic_pointer_cast<LocalFS>(storage).get() != nullptr) {
        local = std::dynamic_pointer_cast<LocalFS>(storage);
    } else {
        throw std::runtime_error("Storage is not LocalFS.");
    }
    if(path_.rfind("file://", 0) != std::string::npos) {
        // remove the scheme.
        path_.erase(0, 7);
    }
    path = std::move(path_);
    raf = local->openRaf(path);
    // TODO: get fileid.
    numRequests = 1;
	asyncNumRequests = 0;
}

std::shared_ptr<ByteBuffer> PhysicalLocalReader::readFully(int length) {
    numRequests++;
    return raf->readFully(length);
}

std::shared_ptr<ByteBuffer> PhysicalLocalReader::readFully(int length, std::shared_ptr<ByteBuffer> bb) {
	numRequests++;
	return raf->readFully(length, bb);
}

void PhysicalLocalReader::close() {
    numRequests++;
    raf->close();
}

long PhysicalLocalReader::getFileLength() {
    numRequests++;
    return raf->length();
}

void PhysicalLocalReader::seek(long desired) {
    numRequests++;
    raf->seek(desired);
}

long PhysicalLocalReader::readLong() {
    return raf->readLong();
}

char PhysicalLocalReader::readChar() {
    return raf->readChar();
}

int PhysicalLocalReader::readInt() {
    return raf->readInt();
}

std::string PhysicalLocalReader::getName() {
    if(path.empty()) {
        return "";
    }
    return path.substr(path.find_last_of('/') + 1);
}

std::shared_ptr<ByteBuffer> PhysicalLocalReader::readAsync(int length, std::shared_ptr<ByteBuffer> buffer, int index) {
	numRequests++;
	if(ConfigFactory::Instance().getProperty("localfs.async.lib") == "iouring") {
		auto directRaf = std::static_pointer_cast<DirectUringRandomAccessFile>(raf);
		return directRaf->readAsync(length, std::move(buffer), index);
	} else if(ConfigFactory::Instance().getProperty("localfs.async.lib") == "aio") {
		throw InvalidArgumentException("PhysicalLocalReader::readAsync: We don't support aio for our async read yet.");
	} else {
		throw InvalidArgumentException("PhysicalLocalReader::readAsync: the async read method is unknown. ");
	}

}

void PhysicalLocalReader::readAsyncSubmit(uint32_t size) {
	numRequests++;
	if(ConfigFactory::Instance().getProperty("localfs.async.lib") == "iouring") {
		auto directRaf = std::static_pointer_cast<DirectUringRandomAccessFile>(raf);
		directRaf->readAsyncSubmit(size);
	} else if(ConfigFactory::Instance().getProperty("localfs.async.lib") == "aio") {
		throw InvalidArgumentException("PhysicalLocalReader::readAsync: We don't support aio for our async read yet.");
	} else {
		throw InvalidArgumentException("PhysicalLocalReader::readAsync: the async read method is unknown. ");
	}
}

void PhysicalLocalReader::readAsyncComplete(uint32_t size) {
	numRequests++;
	if(ConfigFactory::Instance().getProperty("localfs.async.lib") == "iouring") {
		auto directRaf = std::static_pointer_cast<DirectUringRandomAccessFile>(raf);
		directRaf->readAsyncComplete(size);
	} else if(ConfigFactory::Instance().getProperty("localfs.async.lib") == "aio") {
		throw InvalidArgumentException("PhysicalLocalReader::readAsync: We don't support aio for our async read yet.");
	} else {
		throw InvalidArgumentException("PhysicalLocalReader::readAsync: the async read method is unknown. ");
	}
}

void PhysicalLocalReader::readAsyncSubmitAndComplete(uint32_t size){
	numRequests++;
	if(ConfigFactory::Instance().getProperty("localfs.async.lib") == "iouring") {
		auto directRaf = std::static_pointer_cast<DirectUringRandomAccessFile>(raf);
		directRaf->readAsyncSubmit(size);
		::TimeProfiler::Instance().Start("async wait");
		directRaf->readAsyncComplete(size);
		::TimeProfiler::Instance().End("async wait");
	} else if(ConfigFactory::Instance().getProperty("localfs.async.lib") == "aio") {
		throw InvalidArgumentException("PhysicalLocalReader::readAsync: We don't support aio for our async read yet.");
	} else {
		throw InvalidArgumentException("PhysicalLocalReader::readAsync: the async read method is unknown. ");
	}
}
