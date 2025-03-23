//
// Created by yuly on 06.04.23.
//

#ifndef DUCKDB_DATECOLUMNVECTOR_H
#define DUCKDB_DATECOLUMNVECTOR_H

#include "vector/ColumnVector.h"
#include "vector/VectorizedRowBatch.h"

class DateColumnVector: public ColumnVector {
public:
	/*
     * They are the days from 1970-1-1. This is consistent with date type's internal
     * representation in Presto.
	 */
	int * dates;


	/**
    * Use this constructor by default. All column vectors
    * should normally be the default size.
	 */
	explicit DateColumnVector(uint64_t len = VectorizedRowBatch::DEFAULT_SIZE, bool encoding = false);
	~DateColumnVector();
    void * current() override;
	void print(int rowCount) override;
	void close() override;
	void set(int elementNum, int days);

	void add(std::string &value) override;
    void add(int value) override;
    void ensureSize(uint64_t size, bool preserveData) override;
};

#endif // DUCKDB_DATECOLUMNVECTOR_H
