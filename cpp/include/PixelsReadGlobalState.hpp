//
// Created by liyu on 3/26/23.
//

#ifndef EXAMPLE_C_PIXELSREADGLOBALSTATE_HPP
#define EXAMPLE_C_PIXELSREADGLOBALSTATE_HPP

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "PixelsReader.h"
#include "physical/StorageArrayScheduler.h"

namespace duckdb {

struct PixelsReadGlobalState : public GlobalTableFunctionState {
	mutex lock;

	//! The initial reader from the bind phase
	shared_ptr<PixelsReader> initialPixelsReader;

	//! Mutexes to wait for a file that is currently being opened
	unique_ptr<mutex[]> file_mutexes;

	//! Signal to other threads that a file failed to open, letting every thread abort.
	bool error_opening_file = false;

    std::shared_ptr<StorageArrayScheduler> storageArrayScheduler;

	//! Index of file currently up for scanning
	vector<idx_t> file_index;

	//! Batch index of the next row group to be scanned
	idx_t batch_index;

	idx_t max_threads;

    TableFilterSet * filters;

	idx_t MaxThreads() const override {
		return max_threads;
	}
};

}

#endif // EXAMPLE_C_PIXELSREADGLOBALSTATE_HPP
