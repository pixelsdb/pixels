//
// Created by liyu on 3/19/23.
//

#include "reader/IntegerColumnReader.h"

IntegerColumnReader::IntegerColumnReader(std::shared_ptr<TypeDescription> type)
    : ColumnReader(type) {
    isLong = false;
    // TODO: implement
}

void IntegerColumnReader::close() {
    // TODO: implement
}

void IntegerColumnReader::read(std::shared_ptr<ByteBuffer> input,
                               pixels::proto::ColumnEncoding &encoding,
                               int offset, int size, int pixelStride,
                               int vectorIndex,
                               std::shared_ptr<ColumnVector> vector,
                               pixels::proto::ColumnChunkIndex &chunkIndex,
                               std::shared_ptr<PixelsBitMask> filterMask) {
    std::shared_ptr<LongColumnVector> columnVector =
        std::static_pointer_cast<LongColumnVector>(vector);

    // Make sure [offset, offset + size) is in the same pixels.
    assert(offset / pixelStride == (offset + size - 1) / pixelStride);

    // if read from start, init the stream and decoder
    if (offset == 0) {
        decoder = std::make_shared<RunLenIntDecoder>(input, true);
        ColumnReader::elementIndex = 0;
        isLong = type->getCategory() == TypeDescription::Category::LONG;
        isNullOffset = chunkIndex.isnulloffset();
    }

    int pixelId = elementIndex / pixelStride;
    // still need to add pixelsStatistics in the writer
    bool hasNull = chunkIndex.pixelstatistics(pixelId).statistic().hasnull();
    setValid(input, pixelStride, vector, pixelId, hasNull);

    if (encoding.kind() == pixels::proto::ColumnEncoding_Kind_RUNLENGTH) {
        for (int i = 0; i < size; i++) {
            if (isLong) {
                columnVector->longVector[i + vectorIndex] = decoder->next();
            } else {
                *(reinterpret_cast<int *>(columnVector->intVector) + i +
                  vectorIndex) = decoder->next();
            }
            elementIndex++;
        }
    } else {
        if (isLong) {
            std::memcpy(reinterpret_cast<int64_t *>(columnVector->longVector) +
                            vectorIndex,
                        input->getPointer() + input->getReadPos(),
                        size * sizeof(int64_t));
            input->setReadPos(input->getReadPos() + size * sizeof(int64_t));
        } else {
            // if int
            std::memcpy(
                reinterpret_cast<int *>(columnVector->intVector) + vectorIndex,
                input->getPointer() + input->getReadPos(), size * sizeof(int));
            input->setReadPos(input->getReadPos() + size * sizeof(int));
        }
    }
}
