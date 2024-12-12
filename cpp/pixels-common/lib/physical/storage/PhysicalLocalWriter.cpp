//
// Created by pixels on 24-11-25.
//

#include "physical/storage/PhysicalLocalWriter.h"
#include "utils/Constants.h"

PhysicalLocalWriter::PhysicalLocalWriter(const std::string &path, bool overwrite) {
    this->position = 0;
    this->path = path;
    this->rawWriter.open(this->path, overwrite ? std::ios::trunc : std::ios::app);
    if (!this->rawWriter.is_open()) {
        throw std::runtime_error("Failed to open file: " + this->path);
    }
}

std::int64_t PhysicalLocalWriter::prepare(int length) {
    return position;
}

std::int64_t PhysicalLocalWriter::append(const uint8_t *buffer, int offset, int length) {
    std::int64_t start = position;
    rawWriter.write(reinterpret_cast<const char *>(buffer + offset), length);
    position += length;
    return start;
}



void PhysicalLocalWriter::close() {
    rawWriter.close();
}

void PhysicalLocalWriter::flush() {
    rawWriter.flush();
}

std::string PhysicalLocalWriter::getPath() const {
    return path;
}

int PhysicalLocalWriter::getBufferSize() const {
    return Constants::LOCAL_BUFFER_SIZE;
}

std::int64_t PhysicalLocalWriter::append(std::shared_ptr<ByteBuffer> byteBuffer) {
    byteBuffer->filp();
    int length=byteBuffer->bytesRemaining();


    return append(byteBuffer->getPointer(),byteBuffer->getBufferOffset(),length);
}
