//
// Created by pixels on 24-11-25.
//

#ifndef PIXELS_PHYSICALLOCALWRITER_H
#define PIXELS_PHYSICALLOCALWRITER_H

#include "physical/PhysicalWriter.h"
#include "physical/storage/LocalFS.h"
#include "physical/natives/ByteBuffer.h"
#include <fstream>

class PhysicalLocalWriter : public PhysicalWriter {
public:
    PhysicalLocalWriter(const std::string &path, bool overwrite);
    std::int64_t prepare(int length) override;
    std::int64_t append(const uint8_t *buffer, int offset, int length) override;
    std::int64_t append(std::shared_ptr<ByteBuffer> byteBuffer) override;
    void close() override;
    void flush() override;
    std::string getPath() const override;
    int getBufferSize() const override;
private:
    std::shared_ptr<LocalFS> localFS;
    std::string path;
    std::int64_t position;
    std::ofstream rawWriter;
};
#endif //PIXELS_PHYSICALLOCALWRITER_H
