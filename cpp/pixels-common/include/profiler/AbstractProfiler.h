//
// Created by yuly on 03.05.23.
//

#ifndef DUCKDB_ABSTRACTPROFILER_H
#define DUCKDB_ABSTRACTPROFILER_H

constexpr bool enableProfile = true;

class AbstractProfiler {
public:
    virtual void Print() = 0;
    virtual void Reset() = 0;

};

#endif //DUCKDB_ABSTRACTPROFILER_H
