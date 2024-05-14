//
// Created by liyu on 3/19/23.
//

#include "reader/ColumnReader.h"

ColumnReader::ColumnReader(std::shared_ptr<TypeDescription> type) {
    this->type = type;
    this->elementIndex = 0;
}

std::shared_ptr<ColumnReader> ColumnReader::newColumnReader(std::shared_ptr<TypeDescription> type) {
    switch(type->getCategory()) {
        case TypeDescription::BOOLEAN:
            break;
        case TypeDescription::BYTE:
            break;
        case TypeDescription::SHORT:
            break;
        case TypeDescription::INT:
            break;
        case TypeDescription::LONG:
            break;
        case TypeDescription::FLOAT:
            break;
        case TypeDescription::DOUBLE:
            break;
        case TypeDescription::DECIMAL:
            break;
        case TypeDescription::STRING:
            break;
        case TypeDescription::DATE:
            break;
        case TypeDescription::TIME:
            break;
        case TypeDescription::TIMESTAMP:
            break;
        case TypeDescription::VARBINARY:
            break;
        case TypeDescription::BINARY:
            break;
        case TypeDescription::VARCHAR:
            break;
        case TypeDescription::CHAR:
            break;
        case TypeDescription::STRUCT:
            break;
    }
	throw InvalidArgumentException("This function is not supported yet. ");
}

void
ColumnReader::read(std::shared_ptr<ByteBuffer> input, pixels::proto::ColumnEncoding &encoding, int offset, int size,
                   int pixelStride, int vectorIndex, std::shared_ptr<ColumnVector> vector,
                   pixels::proto::ColumnChunkIndex &chunkIndex, std::shared_ptr<PixelsBitMask> filterMask) {
}


void ColumnReader::setValid(const std::shared_ptr<ByteBuffer>& input, int pixelStride, const std::shared_ptr<ColumnVector>& columnVector, int pixelId, bool hasNull) {
    int elementSizeInCurrPixels = std::min(pixelStride, (int)columnVector->length);
    columnVector->isNull = input->getPointer() + isNullOffset;

    int byteSize = ceil(1.0 * elementSizeInCurrPixels / 8);

    if (hasNull) {
        for(int byteOffset = 0; byteOffset < byteSize; byteOffset++) {
            ((uint8_t *) columnVector->isValid)[byteOffset] = ~(columnVector->isNull[byteOffset]);
        }
        isNullOffset += byteSize;
    } else {
        memset(((uint8_t *)columnVector->isValid), 0xFF, byteSize);
    }
//    while (currentElementIndex < initElementIndex + columnVector->length) {
//        int elementSizeInCurrPixels = std::min(pixelStride, (int)(initElementIndex + columnVector->length) - pixelId * pixelStride);
//        elementSizeInCurrPixels = std::min(elementSizeInCurrPixels, pixelStride - (initElementIndex % pixelStride));
//        int byteSize = ceil(1.0 * elementSizeInCurrPixels / 8);
//        if (chunkIndex.pixelstatistics(pixelId).statistic().hasnull()) {
//            for(int byteOffset = 0; byteOffset < byteSize; byteOffset++) {
//                ((uint8_t *) columnVector->isValid)[byteOffset + (currentElementIndex - initElementIndex) / 8] =
//                        ~(columnVector->isNull[byteOffset + nullId]);
//            }
//            nullId += elementSizeInCurrPixels / 8;
//        } else {
//            memset(((uint8_t *)columnVector->isValid) + (currentElementIndex - initElementIndex) / 8, 0xFF, byteSize);
//        }
//        currentElementIndex += elementSizeInCurrPixels;
//        pixelId++;
//    }
//
//    columnVector->isNull = (uint8_t *)(input->getPointer() + isNullOffset + pixelId * pixelStride / 8);
}
