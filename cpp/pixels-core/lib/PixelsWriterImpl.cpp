//
// Created by gengdy on 24-11-25.
//

#include "PixelsWriterImpl.h"
#include "physical/PhysicalWriterUtil.h"
#include "encoding/EncodingLevel.h"
#include <string>

const int PixelsWriterImpl::CHUNK_ALIGNMENT = std::stoi(ConfigFactory::Instance().getProperty("column.chunk.alignment"));

const std::vector<uint8_t> PixelsWriterImpl::CHUNK_PADDING_BUFFER = std::vector<uint8_t>(CHUNK_ALIGNMENT, 0);

PixelsWriterImpl::PixelsWriterImpl(std::shared_ptr<TypeDescription> schema, int pixelsStride, int rowGroupSize,
                                   const std::string &targetFilePath, int blockSize, bool blockPadding,
                                   EncodingLevel encodingLevel, bool nullsPadding, int compressionBlockSize)
                                   : schema(schema), rowGroupSize(rowGroupSize), compressionBlockSize(compressionBlockSize) {
    this->columnWriterOption = std::make_shared<PixelsWriterOption>()->setPixelsStride(pixelsStride)->setEncodingLevel(encodingLevel)->setNullsPadding(nullsPadding);
    this->physicalWriter = PhysicalWriterUtil::newPhysicalWriter(targetFilePath, blockSize, blockPadding, false);
    this->compressionKind = pixels::proto::CompressionKind::NONE;
    // this->timeZone = std::unique_ptr<icu::TimeZone>(icu::TimeZone::createDefault());
    this->children = schema->getChildren();
}

bool PixelsWriterImpl::addRowBatch(std::shared_ptr<VectorizedRowBatch> rowBatch) {
    std::cout << "PixelsWriterImpl::addRowBatch" << std::endl;
    std::vector<std::shared_ptr<ColumnVector>> columnVectors = rowBatch->cols;
    // print columnVectors
    for (int i = 0; i < columnVectors.size(); i++) {
        std::shared_ptr<LongColumnVector> columnVector = std::dynamic_pointer_cast<LongColumnVector>(columnVectors[i]);
        int *data = columnVector->intVector;
        std::cout << "columnVector->writeIndex: " << columnVector->writeIndex << std::endl;
        for(int i = 0; i < columnVector->writeIndex; i++) {
            std::cout << data[i] << " ";
        }
        std::cout << std::endl;
    }
    writeRowGroup();
    writeFileTail();
    return true;
}

void PixelsWriterImpl::writeRowGroup() {
    std::cout << "PixelsWriterImpl::writeRowGroup" << std::endl;
}

void PixelsWriterImpl::writeFileTail() {
    std::cout << "PixelsWriterImpl::writeFileTail" << std::endl;
}