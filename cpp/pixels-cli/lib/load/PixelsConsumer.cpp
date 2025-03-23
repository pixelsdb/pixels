/*
 * Copyright 2024 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
#include "load/PixelsConsumer.h"
#include "encoding/EncodingLevel.h"
#include "utils/ConfigFactory.h"
#include "TypeDescription.h"
#include "vector/ColumnVector.h"
#include "vector/VectorizedRowBatch.h"
#include "physical/storage/LocalFS.h"
#include "PixelsWriterImpl.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <chrono>
#include <regex> // Use C++ standard library regex instead of Boost

int PixelsConsumer::GlobalTargetPathId = 0;

PixelsConsumer::PixelsConsumer(const std::vector<std::string>& queue, const Parameters& parameters,
                              const std::vector<std::string>& loadedFiles)
   : queue(queue), parameters(parameters), loadedFiles(loadedFiles) {}

void PixelsConsumer::run()
{
 std::cout << "Start PixelsConsumer" << std::endl;
 std::string targetPath = parameters.getLoadingPath();
 if (targetPath.back() != '/')
 {
   targetPath += '/';
 }
 std::string schemaStr = parameters.getSchema();
 int maxRowNum = parameters.getMaxRowNum();
 std::string regexPattern = parameters.getRegex();
 EncodingLevel encodingLevel = parameters.getEncodingLevel();
 bool nullPadding = parameters.isNullsPadding();
 if (regexPattern == "\\s")
 {
   regexPattern = " ";
 }

 // Hardcoded values for testing
// int pixelsStride = 2;
// int rowGroupSize = 100;
// int64_t blockSize = 1024;
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
 std::shared_ptr<PixelsWriter> pixelsWriter(nullptr);
 int rowCounter = 0;

 int count = 0;
 for (std::string originalFilePath : queue)
 {
   if (!originalFilePath.empty())
   {
     ++count;
     LocalFS originStorage;
     reader = originStorage.open(originalFilePath);
     if (!reader.is_open())
     {
       std::cerr << "Error opening file: " << originalFilePath << std::endl;
       continue;
     }
     std::cout << "Loading data from: " << originalFilePath << std::endl;

     while (std::getline(reader, line))
     {
       if (line.empty())
       {
         std::cout << "Got empty line" << std::endl;
         continue;
       }

       if (initPixelsFile)
       {
         LocalFS targetStorage;
         targetFileName =
             std::to_string(std::chrono::system_clock::to_time_t(std::chrono::system_clock::now())) +
             ".pxl";
         targetFilePath = targetPath + targetFileName;
         pixelsWriter = std::make_shared<PixelsWriterImpl>(schema, pixelsStride, rowGroupSize,
                                                           targetFilePath, blockSize,
                                                           true, encodingLevel, nullPadding, false, 1);
       }
       initPixelsFile = false;

       ++rowBatch->rowCount;
       ++rowCounter;

       std::vector<std::string> colsInLine;
       std::regex regex(regexPattern);
       std::sregex_token_iterator it(line.begin(), line.end(), regex, -1);
       std::sregex_token_iterator end;
       for (; it != end; ++it)
       {
         colsInLine.push_back(*it);
       }
       for (int i = 0; i < columnVectors.size(); ++i)
       {
         if (i >= colsInLine.size() || colsInLine[i].empty() || colsInLine[i] == "\\N")
         {
           columnVectors[i]->addNull();
         }
         else
         {
           columnVectors[i]->add(colsInLine[i]);
         }
       }

       if (rowBatch->rowCount == rowBatch->getMaxSize())
       {
         std::cout << "Writing row group to file: " << targetFilePath << " rowCount:" << rowBatch->rowCount
                   << std::endl;
         pixelsWriter->addRowBatch(rowBatch);

         rowBatch->reset();
       }

       // Create a new file if row count exceeds the limit
       if (rowCounter >= maxRowNum)
       {
         if (rowBatch->rowCount != 0)
         {
           pixelsWriter->addRowBatch(rowBatch);
           rowBatch->reset();
         }
         pixelsWriter->close();
         this->loadedFiles.push_back(targetFilePath);
         std::cout << "Generate file: " << targetFilePath << std::endl;
         rowCounter = 0;
         initPixelsFile = true;
       }
     }
   }
 }
 // Write remaining lines to the file
 if (rowCounter > 0)
 {
   if (rowBatch->rowCount != 0)
   {
     pixelsWriter->addRowBatch(rowBatch);
     rowBatch->reset();
   }
   pixelsWriter->close();
   this->loadedFiles.push_back(targetFilePath);
 }
 std::cout <<"Exit PixelsConsumer" << std::endl;
}