//
// Created by gengdy on 24-11-25.
//

#ifndef PIXELS_PHYSICALWRITER_H
#define PIXELS_PHYSICALWRITER_H

#include <cstdint>
#include <string>

class PhysicalWriter {
public:
    virtual ~PhysicalWriter() = default;
    /**
     * Prepare the writer to ensure the length can fit into current block.
     *
     * @param length length of content
     * @return starting offset after preparing. If -1, means prepare has failed,
     * due to the specified length cannot fit into current block.
     */
    virtual std::int64_t prepare(int length) = 0;
    /**
     * Append content to the file.
     *
     * @param buffer content buffer container
     * @param offset start offset of actual content buffer
     * @param length length of actual content buffer
     * @return start offset of content in the file.
     */
    virtual std::int64_t append(const uint8_t *buffer, int offset, int length) = 0;
    /**
     * Close writer.
     */
    virtual void close() = 0;
    /**
     * Flush writer.
     */
    virtual void flush() = 0;

    virtual std::string getPath() const = 0;

    virtual int getBufferSize() const = 0;
};
#endif //PIXELS_PHYSICALWRITER_H
