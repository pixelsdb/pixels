//
// Created by liyu on 1/24/24.
//

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
