//
// Created by yuly on 05.04.23.
//

#ifndef PIXELS_DECIMALCOLUMNREADER_H
#define PIXELS_DECIMALCOLUMNREADER_H

#include "reader/ColumnReader.h"

class DecimalColumnReader: public ColumnReader {
public:
    explicit DecimalColumnReader(std::shared_ptr<TypeDescription> type);
    void close() override;
    void read(std::shared_ptr<ByteBuffer> input,
              pixels::proto::ColumnEncoding & encoding,
              int offset, int size, int pixelStride,
              int vectorIndex, std::shared_ptr<ColumnVector> vector,
              pixels::proto::ColumnChunkIndex & chunkIndex,
              std::shared_ptr<pixelsFilterMask> filterMask) override;
private:
    /**
     * True if the data type of the values is long (int64), otherwise the data type is int32.
     */
    bool isLong;
};

#endif //PIXELS_DECIMALCOLUMNREADER_H
