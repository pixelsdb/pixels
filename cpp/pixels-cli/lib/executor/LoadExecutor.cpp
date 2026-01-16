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
#include <fstream>
#include <filesystem>
#include <encoding/EncodingLevel.h>
#include <physical/storage/LocalFS.h>
#include <load/Parameters.h>
#include <chrono>
#include <thread>
#include <load/PixelsConsumer.h>

void LoadExecutor::execute(const bpo::variables_map &ns, const std::string &command)
{
    std::string schema = ns["schema"].as<std::string>();
    if (std::filesystem::exists(schema) && std::filesystem::is_regular_file(schema))
    {
        std::ifstream ifs(schema);
        if (ifs.is_open())
        {
            std::stringstream buffer;
            buffer << ifs.rdbuf();
            schema = buffer.str();
        }
    }
    std::string origin = ns["origin"].as<std::string>();
    std::string target = ns["target"].as<std::string>();
    int rowNum = ns["row_num"].as<int>();
    std::string regex = ns["row_regex"].as<std::string>();
    EncodingLevel encodingLevel = EncodingLevel::from(ns["encoding_level"].as<int>());
    bool nullPadding = ns["nulls_padding"].as<bool>();
    int concurrency = ns["concurrency"].as<int>();

    if (origin.back() != '/')
    {
        origin += "/";
    }

    Parameters parameters(schema, rowNum, regex, target, encodingLevel, nullPadding);
    LocalFS localFs;
    std::vector <std::string> fileList = localFs.listPaths(origin);
    std::vector <std::string> inputFiles, loadedFiles;
    for (auto filePath: fileList)
    {
        inputFiles.push_back(localFs.ensureSchemePrefix(filePath));
    }

    auto startTime = std::chrono::system_clock::now();
    if (startConsumers(inputFiles, parameters, loadedFiles, concurrency))
    {
        std::cout << command << " is successful" << std::endl;
    }
    else
    {
        std::cout << command << " failed" << std::endl;
    }
    auto endTime = std::chrono::system_clock::now();
    std::chrono::duration<double> elapsedSeconds = endTime - startTime;
    std::cout << "Text file in " << origin << " are loaded by " << concurrency << " thread(s) in "
              << elapsedSeconds.count() << " seconds." << std::endl;
}

bool LoadExecutor::startConsumers(const std::vector <std::string> &inputFiles, Parameters parameters,
                                  const std::vector <std::string> &loadedFiles, int concurrency)
{
    if (concurrency <= 1 || inputFiles.size() <= 1)
    {
        // Single-threaded mode
        PixelsConsumer consumer(inputFiles, parameters, loadedFiles);
        consumer.run();
    }
    else
    {
        // Multi-threaded mode: each thread processes one file
        std::vector<std::thread> threads;
        int numThreads = std::min(concurrency, static_cast<int>(inputFiles.size()));
        std::vector<std::vector<std::string>> inputfilesQueue(numThreads);
        int currentThread=0;
        for (int i = 0; i < inputFiles.size(); ++i)
        {
            inputfilesQueue[(currentThread++)%numThreads].push_back(inputFiles[i]);
        }
        // Each thread gets one file queue to process
        for (int i=0;i<numThreads;i++)
        {
            auto queue=inputfilesQueue[i];
            threads.emplace_back([queue, parameters, loadedFiles]() {
                PixelsConsumer consumer(queue, parameters, loadedFiles);
                consumer.run();
            });
        }
        // Wait for all threads to complete
        for (auto &thread : threads)
        {
            if (thread.joinable())
            {
                thread.join();
            }
        }
    }
    return true;
}
