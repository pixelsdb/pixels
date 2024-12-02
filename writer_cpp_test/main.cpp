//
// Created by whz on 12/2/24.
//
#include "PixelsWriterImpl.h"
#include "TypeDescription.h"
#include "vector/VectorizedRowBatch.h"
#include "vector/LongColumnVector.h"
#include "physical/storage/LocalFS.h"
#include "encoding/EncodingLevel.h"
#include <iostream>
#include <string>
#include <vector>
#include <regex>

int main(){
    std::cout<<"Hello PixelsWriter!"<<std::endl;
    // prepare parameters
    std::string inputFilePath="";
    std::string targetFilePath="";
    int pixelsStride=0;
    int rowGroupSize=0;
    int blockSize=0;
    bool blockPading= true;
    EncodingLevel encodingLevel;
    bool nullPadding= false;
    bool partitioned= false;
    int compressionBlockSize=1;
    std::string schemaStr="";
    int maxRowNum=0;
    std::shared_ptr<TypeDescription> schema = TypeDescription::fromString(schemaStr);
    std::shared_ptr<VectorizedRowBatch> rowBatch = schema->createRowBatch(pixelsStride);
    std::vector<std::shared_ptr<ColumnVector>> columnVectors = rowBatch->cols;
    std::ifstream reader;
    std::string line;
    bool initPixelsFile = true;
    std::string targetFileName = "";
    std::shared_ptr<PixelsWriter> pixelsWriter(nullptr);
    int rowCounter = 0;

    LocalFS originStorage;
    reader = originStorage.open(inputFilePath);
    if (!reader.is_open()) {
        std::cerr << "Error opening file: " << inputFilePath << std::endl;
    }
    std::cout << "loading data from: " << inputFilePath<< std::endl;
    while (std::getline(reader, line)) {
        if (initPixelsFile) {
            if (line.empty()) {
                std::cout << "got empty line" << std::endl;
                continue;
            }
            LocalFS targetStorage;
            pixelsWriter = std::make_shared<PixelsWriterImpl>(schema, pixelsStride, rowGroupSize, targetFilePath, blockSize,
                                                              true, encodingLevel, nullPadding, false,1);
        }
        initPixelsFile = false;

        ++rowBatch->rowCount;
        ++rowCounter;
        std::string regex="\|";
        std::vector<std::string> colsInLine;
        std::regex re(regex);
        std::sregex_token_iterator it(line.begin(), line.end(), re, -1);
        std::sregex_token_iterator end;
        while (it!= end) {
            colsInLine.push_back(*it++);
        }
        for(int i = 0; i < columnVectors.size(); ++i) {
            if (i > colsInLine.size() || colsInLine[i].empty() || colsInLine[i] == "\\N") {
                columnVectors[i]->addNull();
            } else {
                columnVectors[i]->add(colsInLine[i]);
            }
        }

        if (rowBatch->rowCount >= rowBatch->getMaxSize()) {
            std::cout << "writing row group to file: " << targetFilePath << std::endl;
            pixelsWriter->addRowBatch(rowBatch);
            rowBatch->reset();
        }

        if (rowCounter >= maxRowNum) {
            if (rowBatch->rowCount != 0) {
                pixelsWriter->addRowBatch(rowBatch);
                rowBatch->reset();
            }
            rowCounter = 0;
            initPixelsFile = true;
        }
    }
}