//
// Created by gengdy on 24-11-22.
//

#include "load/PixelsConsumer.h"
#include "encoding/EncodingLevel.h"
#include "utils/ConfigFactory.h"
#include "TypeDescription.h"
#include "vector/ColumnVector.h"
#include "vector/VectorizedRowBatch.h"
#include "physical/storage/LocalFS.h"
#include <boost/regex.hpp>
#include <iostream>
#include <fstream>
#include <sstream>
#include <chrono>

int PixelsConsumer::GlobalTargetPathId = 0;

PixelsConsumer::PixelsConsumer(const std::vector <std::string> &queue, const Parameters &parameters,
                               const std::vector <std::string> &loadedFiles)
                               : queue(queue), parameters(parameters), loadedFiles(loadedFiles) {}

void PixelsConsumer::run() {
    std::cout << "Start PixelsConsumer" << std::endl;
    std::string targetPath = parameters.getLoadingPath();
    if (targetPath.back() != '/') {
        targetPath += '/';
    }
    std::string schemaStr = parameters.getSchema();
    int maxRowNum = parameters.getMaxRowNum();
    std::string regex = parameters.getRegex();
    EncodingLevel encodingLevel = parameters.getEncodingLevel();
    bool nullPadding = parameters.isNullsPadding();
    if (regex == "\\s") {
        regex = " ";
    }

    int pixelsStride = std::stoi(ConfigFactory::Instance().getProperty("pixel.stride"));
    int rowGroupSize = std::stoi(ConfigFactory::Instance().getProperty("row.group.size"));
    int64_t blockSize = std::stoll(ConfigFactory::Instance().getProperty("block.size"));
    short replication = static_cast<short>(std::stoi(ConfigFactory::Instance().getProperty("block.replication")));

    std::shared_ptr<TypeDescription> schema = TypeDescription::fromString(schemaStr);
    std::shared_ptr<VectorizedRowBatch> rowBatch = schema->createRowBatch(pixelsStride);
    std::vector<std::shared_ptr<ColumnVector>> columnVectors = rowBatch->cols;

    std::ifstream reader;
    std::string line;

    bool initPixelsFile = true;
    std::string targetFileName = "";
    std::string targetFilePath;
    int rowCounter = 0;

    int count = 0;
    for (std::string originalFilePath : queue) {
        if (!originalFilePath.empty()) {
            ++count;
            LocalFS originStorage;
            reader = originStorage.open(originalFilePath);
            if (!reader.is_open()) {
                std::cerr << "Error opening file: " << originalFilePath << std::endl;
                continue;
            }
            std::cout << "loading data from: " << originalFilePath << std::endl;

            while (std::getline(reader, line)) {
                if (initPixelsFile) {
                    if (line.empty()) {
                        std::cout << "got empty line" << std::endl;
                        continue;
                    }
                    LocalFS targetStorage;
                    targetFileName = std::to_string(std::chrono::system_clock::to_time_t(std::chrono::system_clock::now())) + ".pxl";
                    targetFilePath = targetPath + targetFileName;
                }
                initPixelsFile = false;

                ++rowBatch->rowCount;
                ++rowCounter;

                std::vector<std::string> colsInLine;
                boost::sregex_token_iterator it(line.begin(), line.end(), boost::regex(regex), -1);
                for (; it != boost::sregex_token_iterator(); ++it) {
                    colsInLine.push_back(*it);
                }
                for(int i = 0; i < columnVectors.size(); ++i) {
                    if (i > colsInLine.size() || colsInLine[i].empty() || colsInLine[i] == "\\N") {
                        std::cout << "adding null to column: " << i << std::endl;
                        columnVectors[i]->addNull();
                    } else {
                        std::cout << "adding value " << colsInLine[i] << " to column: " << i << std::endl;
                        columnVectors[i]->add(colsInLine[i]);
                    }
                }

                if (rowBatch->rowCount >= rowBatch->getMaxSize()) {
                    std::cout << "writing row group to file: " << targetFilePath << std::endl;
                    // pixelsWriter->addRowBatch(rowBatch);
                    rowBatch->reset();
                }

                if (rowCounter >= maxRowNum) {
                    if (rowBatch->rowCount != 0) {
                        // pixelsWriter->addRowBatch(rowBatch);
                        rowBatch->reset();
                    }
                    this->loadedFiles.push_back(targetFilePath);
                    rowCounter = 0;
                    initPixelsFile = true;
                }
            }
        }
    }
    if (rowCounter > 0) {
        if (rowBatch->rowCount != 0) {
            // pixelsWriter->addRowBatch(rowBatch);
            rowBatch->reset();
        }
        this->loadedFiles.push_back(targetFilePath);
    }
    std::cout << "Exit PixelsConsumer" << std::endl;
}