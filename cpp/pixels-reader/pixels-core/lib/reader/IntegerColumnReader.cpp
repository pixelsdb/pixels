//
// Created by liyu on 3/19/23.
//

#include "reader/IntegerColumnReader.h"

IntegerColumnReader::IntegerColumnReader(std::shared_ptr<TypeDescription> type) : ColumnReader(type) {
    isLong = false;
    // TODO: implement
}

void IntegerColumnReader::close() {
    // TODO: implement
}

void IntegerColumnReader::read(std::shared_ptr<ByteBuffer> input, pixels::proto::ColumnEncoding & encoding, int offset,
                               int size, int pixelStride, int vectorIndex, std::shared_ptr<ColumnVector> vector,
                               pixels::proto::ColumnChunkIndex & chunkIndex, std::shared_ptr<pixelsFilterMask> filterMask) {
    std::shared_ptr<LongColumnVector> columnVector =
            std::static_pointer_cast<LongColumnVector>(vector);
    // if read from start, init the stream and decoder
    if(offset == 0) {
        decoder = std::make_shared<RunLenIntDecoder>(input, true);
        ColumnReader::elementIndex = 0;
		isLong = type->getCategory() == TypeDescription::Category::LONG;
    }
    if(encoding.kind() == pixels::proto::ColumnEncoding_Kind_RUNLENGTH) {
        for(int i = 0; i < size; i++) {
			if(isLong) {
				columnVector->longVector[i + vectorIndex] = decoder->next();
			} else {
				columnVector->intVector[i + vectorIndex] = decoder->next();
			}
            elementIndex++;
        }
    } else {
        if(isLong) {
            // if long
			columnVector->longVector = (int64_t *)(input->getPointer() + input->getReadPos());
			input->setReadPos(input->getReadPos() + size * sizeof(int64_t));
        } else {
            // if int
			columnVector->intVector = (int *)(input->getPointer() + input->getReadPos());
			input->setReadPos(input->getReadPos() + size * sizeof(int));
        }
    }
}
