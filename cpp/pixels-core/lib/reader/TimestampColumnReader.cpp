//
// Created by liyu on 12/23/23.
//

#include "reader/TimestampColumnReader.h"

TimestampColumnReader::TimestampColumnReader(std::shared_ptr<TypeDescription> type) : ColumnReader(type) {

}

void TimestampColumnReader::close() {

}

void TimestampColumnReader::read(std::shared_ptr<ByteBuffer> input, pixels::proto::ColumnEncoding &encoding, int offset,
                                 int size, int pixelStride, int vectorIndex, std::shared_ptr<ColumnVector> vector,
                                 pixels::proto::ColumnChunkIndex &chunkIndex,
                                 std::shared_ptr<PixelsBitMask> filterMask) {
    std::shared_ptr<TimestampColumnVector> columnVector =
            std::static_pointer_cast<TimestampColumnVector>(vector);
    // if read from start, init the stream and decoder
    if(offset == 0) {
        decoder = std::make_shared<RunLenIntDecoder>(input, true);
        ColumnReader::elementIndex = 0;
    }
    if(encoding.kind() == pixels::proto::ColumnEncoding_Kind_RUNLENGTH) {
        for (int i = 0; i < size; i++) {
            if (elementIndex % pixelStride == 0) {
                int pixelId = elementIndex / pixelStride;
            }
            columnVector->set(i + vectorIndex, decoder->next());
            elementIndex++;
        }
    } else {
        columnVector->times = (int64_t *)(input->getPointer() + input->getReadPos());
        input->setReadPos(input->getReadPos() + size * sizeof(int64_t));
    }
}
