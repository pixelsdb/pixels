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
    std::string schemaStr = parameters.getSchema();
    int maxRows = parameters.getMaxRowNum();
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
    std::string currTargetPath = "";
    int rowCounter = 0;

    bool isRunning = true;
    int count = 0;
    while (isRunning) {
        for (std::string originalFilePath : queue) {
            if (originalFilePath != "") {
                ++count;
                LocalFS originStorage;
                reader = originStorage.open(originalFilePath);
                if (!reader.is_open()) {
                    std::cerr << "Error opening file: " << originalFilePath << std::endl;
                    continue;
                }
                std::cout << "loading data from: " << originalFilePath << std::endl;

                while (std::getline(reader, line)) {
                    std::cout << line << std::endl;
                }
            }
        }
        isRunning = false;
    }
}