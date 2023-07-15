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
	shared_ptr<PixelsRecordReader> currPixelsRecordReader;
    shared_ptr<PixelsRecordReader> nextPixelsRecordReader;
	// this is used for storing row batch results.
	shared_ptr<VectorizedRowBatch> vectorizedRowBatch;
	int rowOffset;
	vector<column_t> column_ids;
	vector<string> column_names;
	shared_ptr<PixelsReader> currReader;
    shared_ptr<PixelsReader> nextReader;
	idx_t curr_file_index;
    idx_t next_file_index;
    idx_t curr_batch_index;
    idx_t next_batch_index;
    std::string next_file_name;
    std::string curr_file_name;
    // the state when PixelsScanInitLocal calls
    bool is_first_state;
    // the state when next_file_index is none. In this state we only readBatch the curr_file.
    bool is_last_state;
};

}

#endif // EXAMPLE_C_PIXELSREADLOCALSTATE_HPP
