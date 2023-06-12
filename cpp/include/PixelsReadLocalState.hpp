//
// Created by liyu on 3/26/23.
//

#ifndef EXAMPLE_C_PIXELSREADLOCALSTATE_HPP
#define EXAMPLE_C_PIXELSREADLOCALSTATE_HPP

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "PixelsReader.h"
#include "reader/PixelsRecordReader.h"

namespace duckdb {

struct PixelsReadLocalState : public LocalTableFunctionState {
	shared_ptr<PixelsRecordReader> pixelsRecordReader;
	// this is used for storing row batch results.
	shared_ptr<VectorizedRowBatch> vectorizedRowBatch;
	std::vector<shared_ptr<VectorizedRowBatch>> vectorizedRowBatchPool;
	int rowOffset;
	vector<column_t> column_ids;
	vector<string> column_names;
	shared_ptr<PixelsReader> reader;
	idx_t file_index;
	idx_t batch_index;
};

}

#endif // EXAMPLE_C_PIXELSREADLOCALSTATE_HPP
