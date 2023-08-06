//
// Created by liyu on 3/19/23.
//

#ifndef PIXELS_COLUMNREADER_H
#define PIXELS_COLUMNREADER_H

#include "TypeDescription.h"
#include "physical/natives/ByteBuffer.h"
#include "pixels-common/pixels.pb.h"
#include "math.h"
#include "duckdb.h"
#include "duckdb/common/types/vector.hpp"
#include "PixelsFilter.h"

class ColumnReader {
public:
    ColumnReader(std::shared_ptr<TypeDescription> type);
    static std::shared_ptr<ColumnReader> newColumnReader(std::shared_ptr<TypeDescription> type);
    /**
       * Closes this column reader and releases any resources associated
       * with it. If the column reader is already closed then invoking this
       * method has no effect.
       */
    virtual void close() = 0;

    /**
     * Read values from input buffer.
     * Values after specified offset are gonna be put into the specified vector.
     *
     * @param input    input buffer
     * @param encoding encoding type
     * @param offset   starting reading offset of values
     * @param size     number of values to read
     * @param pixelStride the stride (number of rows) in a pixels.
     * @param vectorIndex the index from where we start reading values into the vector
     * @param vector   vector to read values into
     * @param chunkIndex the metadata of the column chunk to read.
     */
    virtual void read(std::shared_ptr<ByteBuffer> input,
                      pixels::proto::ColumnEncoding & encoding,
                      int offset, int size, int pixelStride,
                      int vectorIndex, std::shared_ptr<ColumnVector> vector,
                      pixels::proto::ColumnChunkIndex & chunkIndex,
                      std::shared_ptr<PixelsBitMask> filterMask) = 0;
private:
	bool hasNull;
protected:
    int elementIndex;
	std::shared_ptr<TypeDescription> type;
};
#endif //PIXELS_COLUMNREADER_H
