 //
// Created by yuly on 06.04.23.
//

#include "reader/DateColumnReader.h"


DateColumnReader::DateColumnReader(std::shared_ptr<TypeDescription> type) : ColumnReader(type) {
	// TODO: implement
}

void DateColumnReader::close() {

}

void DateColumnReader::read(std::shared_ptr<ByteBuffer> input, pixels::proto::ColumnEncoding & encoding, int offset,
                               int size, int pixelStride, int vectorIndex, std::shared_ptr<ColumnVector> vector,
                               pixels::proto::ColumnChunkIndex & chunkIndex, std::shared_ptr<PixelsBitMask> filterMask) {
	std::shared_ptr<DateColumnVector> columnVector =
	    std::static_pointer_cast<DateColumnVector>(vector);
	if(offset == 0) {
		decoder = std::make_shared<RunLenIntDecoder>(input, true);
		elementIndex = 0;
	}
	if(encoding.kind() == pixels::proto::ColumnEncoding_Kind_RUNLENGTH) {
        for (int i = 0; i < size; i++) {
            if (elementIndex % pixelStride == 0) {
                int pixelId = elementIndex / pixelStride;
            }
            columnVector->set(i + vectorIndex, (int) decoder->next());
            elementIndex++;
        }
	} else {
		columnVector->dates = (int *)(input->getPointer() + input->getReadPos());
		input->setReadPos(input->getReadPos() + size * sizeof(int));
	}
}
