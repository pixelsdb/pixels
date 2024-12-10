//
// Created by whz on 12/9/24.
//

#ifndef DUCKDB_TIMESTAMPCOLUMNWRITER_H
#define DUCKDB_TIMESTAMPCOLUMNWRITER_H
#include "ColumnWriter.h"
#include "encoding/RunLenIntEncoder.h"

class TimestampColumnWriter : public ColumnWriter{
    TimestampColumnWriter(std::shared_ptr<TypeDescription> type, std::shared_ptr<PixelsWriterOption> writerOption);

    int write(std::shared_ptr<ColumnVector> vector, int length) override;
    void close() override;
    void newPixel() override;
    void writeCurPartTimestamp(std::shared_ptr<ColumnVector> columnVector, long* values, int curPartLength, int curPartOffset);
    bool decideNullsPadding(std::shared_ptr<PixelsWriterOption> writerOption) override;
    pixels::proto::ColumnEncoding getColumnChunkEncoding() const;
private:
    bool runlengthEncoding;
    std::unique_ptr<RunLenIntEncoder> encoder;
    std::vector<long> curPixelVector; // current pixel value vector haven't written out yet

};
#endif //DUCKDB_TIMESTAMPCOLUMNWRITER_H
