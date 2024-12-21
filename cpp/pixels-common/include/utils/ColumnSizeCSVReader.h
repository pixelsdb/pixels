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
 * @author liyu
 * @create 2024-01-24
 */
#ifndef DUCKDB_COLUMNSIZECSVREADER_H
#define DUCKDB_COLUMNSIZECSVREADER_H

#include <iostream>
#include <fstream>
#include <string>
#include <unordered_map>
#include "exception/InvalidArgumentException.h"

class ColumnSizeCSVReader {
public:
    ColumnSizeCSVReader(std::string csvPath) {
        std::ifstream file;
        file.open(csvPath);
        std::string line;
        while (getline(file, line)) {
            std::string delimiter = " ";
            std::string columnName = line.substr(0, line.find(delimiter));
            int maxSize = std::stoi(line.substr(line.find(delimiter) + 1));
            colSize[columnName] = maxSize;
        }
        file.close();
    }
    int get(const std::string & columnName);
private:
    std::unordered_map<std::string, int> colSize;
};

#endif //DUCKDB_COLUMNSIZECSVREADER_H
