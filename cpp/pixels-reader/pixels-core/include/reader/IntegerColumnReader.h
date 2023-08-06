//
// Created by liyu on 3/19/23.
//

#ifndef PIXELS_INTEGERCOLUMNREADER_H
#define PIXELS_INTEGERCOLUMNREADER_H

#include "reader/ColumnReader.h"
#include "encoding/RunLenIntDecoder.h"

class IntegerColumnReader: public ColumnReader {
public:
    explicit IntegerColumnReader(std::shared_ptr<TypeDescription> type);
    void close() override;
    void read(std::shared_ptr<ByteBuffer> input,
              pixels::proto::ColumnEncoding &encoding,
              int offset, int size, int pixelStride,
              int vectorIndex, std::shared_ptr<ColumnVector> vector,
              pixels::proto::ColumnChunkIndex & chunkIndex,
              std::shared_ptr<PixelsBitMask> filterMask) override;
private:
    /**
     * True if the data type of the values is long (int64), otherwise the data type is int32.
     */
    bool isLong;
    std::shared_ptr<RunLenIntDecoder> decoder;
};


#endif //PIXELS_INTEGERCOLUMNREADER_H
