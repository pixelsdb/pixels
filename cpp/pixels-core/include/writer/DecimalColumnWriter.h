//
// Created by whz on 12/9/24.
//

#ifndef DUCKDB_DECIMALCOLUMNWRITER_H
#define DUCKDB_DECIMALCOLUMNWRITER_H
#include "encoding/RunLenIntEncoder.h"
#include "ColumnWriter.h"
#include "utils/EncodingUtils.h"

class DecimalColumnWriter :public  ColumnWriter{
public:
    DecimalColumnWriter(std::shared_ptr<TypeDescription> type,std::shared_ptr<PixelsWriterOption> writerOption);
    int write(std::shared_ptr<ColumnVector> vector, int length) override;
    bool decideNullsPadding(std::shared_ptr<PixelsWriterOption> writerOption) override;
};

#endif //DUCKDB_DECIMALCOLUMNWRITER_H
