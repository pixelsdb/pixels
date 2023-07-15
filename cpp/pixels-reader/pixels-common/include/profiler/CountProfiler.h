//
// Created by yuly on 03.05.23.
//

#ifndef DUCKDB_COUNTPROFILER_H
#define DUCKDB_COUNTPROFILER_H
#include <iostream>
#include <memory>
#include <string>
#include "exception/InvalidArgumentException.h"
#include "profiler/AbstractProfiler.h"
#include <chrono>
#include <map>
#include <mutex>


// This class is used for showing how many times a function is invoked.

class CountProfiler: public AbstractProfiler {
public:
    static CountProfiler & Instance();
    void Count(const std::string& label);
	void Count(const std::string& label, int num);
    void Print() override;
    void Reset() override;
    long Get(const std::string& label);
private:
    std::mutex lock;
    std::map<std::string, long> result;
};




#endif //DUCKDB_COUNTPROFILER_H
