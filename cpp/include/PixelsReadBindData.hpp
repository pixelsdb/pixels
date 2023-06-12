//
// Created by liyu on 3/27/23.
//

#ifndef EXAMPLE_C_PIXELSREADBINDDATA_HPP
#define EXAMPLE_C_PIXELSREADBINDDATA_HPP


#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "PixelsReader.h"


namespace duckdb {

struct PixelsReadBindData : public TableFunctionData {
	shared_ptr<PixelsReader> initialPixelsReader;
	shared_ptr<TypeDescription> fileSchema;
	vector<string> files;
	atomic<idx_t> curFileId;
};

}
#endif // EXAMPLE_C_PIXELSREADBINDDATA_HPP
