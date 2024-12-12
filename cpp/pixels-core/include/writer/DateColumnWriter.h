//
// Created by whz on 11/19/24.
//

#ifndef DUCKDB_DATECOLUMNWRITER_H
#define DUCKDB_DATECOLUMNWRITER_H

#include "ColumnWriter.h"
#include "encoding/RunLenIntEncoder.h"

class DateColumnWriter : public ColumnWriter{
    DateColumnWriter(std::shared_ptr<TypeDescription> type, std::shared_ptr<PixelsWriterOption> writerOption);

    int write(std::shared_ptr<ColumnVector> vector, int length) override;
    void close() override;
    void newPixel() override;
    void writeCurPartTime(std::shared_ptr<ColumnVector> columnVector, long* values, int curPartLength, int curPartOffset);
    bool decideNullsPadding(std::shared_ptr<PixelsWriterOption> writerOption) override;
    pixels::proto::ColumnEncoding getColumnChunkEncoding() const;

private:
    bool runlengthEncoding;
    std::unique_ptr<RunLenIntEncoder> encoder;
    std::vector<long> curPixelVector; // current pixel value vector haven't written out yet

};
#endif // DUCKDB_DATECOLUMNWRITER_H
