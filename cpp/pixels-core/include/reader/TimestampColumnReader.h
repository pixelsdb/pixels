//
// Created by liyu on 12/23/23.
//

#ifndef DUCKDB_TIMESTAMPCOLUMNREADER_H
#define DUCKDB_TIMESTAMPCOLUMNREADER_H

#include "reader/ColumnReader.h"
#include "encoding/RunLenIntDecoder.h"

class TimestampColumnReader: public ColumnReader {
public:
    explicit TimestampColumnReader(std::shared_ptr<TypeDescription> type);
    void close() override;
    void read(std::shared_ptr<ByteBuffer> input,
              pixels::proto::ColumnEncoding & encoding,
              int offset, int size, int pixelStride,
              int vectorIndex, std::shared_ptr<ColumnVector> vector,
              pixels::proto::ColumnChunkIndex & chunkIndex,
              std::shared_ptr<PixelsBitMask> filterMask) override;

private:
    std::shared_ptr<RunLenIntDecoder> decoder;
};

#endif //DUCKDB_TIMESTAMPCOLUMNREADER_H
