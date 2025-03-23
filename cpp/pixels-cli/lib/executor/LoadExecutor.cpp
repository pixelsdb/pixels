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

/*
 * @author gengdy
 * @create 2024-11-17
 */
#include <executor/LoadExecutor.h>
#include <iostream>
#include <encoding/EncodingLevel.h>
#include <physical/storage/LocalFS.h>
#include <load/Parameters.h>
#include <chrono>
#include <load/PixelsConsumer.h>

void LoadExecutor::execute(const std::map<std::string, std::string>& options, const std::string& command)
{
  // Extract parameters from the options map
  // schema s
  std::string schema = options.at("s");
  // origin o
  std::string origin = options.at("o");
  // target t
  std::string target = options.at("t");
  // row_num n
  int rowNum = std::stoi(options.at("n"));
  // row_regex r
  std::string regex = options.at("r");
  // encoding_level e (default value: 2)
  int encodingLevelValue = options.count("e") ? std::stoi(options.at("e")) : 2;
  EncodingLevel encodingLevel = EncodingLevel::from(encodingLevelValue);
  // null_padding p (default value: false)
  bool nullPadding = options.count("p") ? options.at("p") == "true" : false;

  // Ensure origin path ends with '/'
  if (origin.back() != '/')
  {
    origin += "/";
  }

  // Create Parameters object
  Parameters parameters(schema, rowNum, regex, target, encodingLevel, nullPadding);

  // List files in the origin directory
  LocalFS localFs;
  std::vector<std::string> fileList = localFs.listPaths(origin);
  std::vector<std::string> inputFiles, loadedFiles;
  for (const auto& filePath : fileList)
  {
    inputFiles.push_back(localFs.ensureSchemePrefix(filePath));
  }

  // Start processing files
  auto startTime = std::chrono::system_clock::now();
  if (startConsumers(inputFiles, parameters, loadedFiles))
  {
    std::cout << command << " is successful" << std::endl;
  }
  else
  {
    std::cout << command << " failed" << std::endl;
  }
  auto endTime = std::chrono::system_clock::now();
  std::chrono::duration<double> elapsedSeconds = endTime - startTime;
  std::cout << "Text files in " << origin << " are loaded by 1 thread in "
            << elapsedSeconds.count() << " seconds." << std::endl;
}

bool LoadExecutor::startConsumers(const std::vector<std::string>& inputFiles, Parameters parameters,
                                  const std::vector<std::string>& loadedFiles)
{
  PixelsConsumer consumer(inputFiles, parameters, loadedFiles);
  consumer.run();
  return true;
}