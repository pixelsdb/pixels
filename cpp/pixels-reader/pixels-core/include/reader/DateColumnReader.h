//
// Created by yuly on 06.04.23.
//

#ifndef DUCKDB_DATECOLUMNREADER_H
#define DUCKDB_DATECOLUMNREADER_H

#include "reader/ColumnReader.h"
#include "encoding/RunLenIntDecoder.h"

class DateColumnReader: public ColumnReader {
public:
	explicit DateColumnReader(std::shared_ptr<TypeDescription> type);
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
	std::shared_ptr<RunLenIntDecoder> decoder;
};


#endif // DUCKDB_DATECOLUMNREADER_H
