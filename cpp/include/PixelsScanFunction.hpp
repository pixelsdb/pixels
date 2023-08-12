//
// Created by liyu on 3/26/23.
//
#pragma once

#ifndef EXAMPLE_C_PIXELSSCANFUNCTION_HPP
#define EXAMPLE_C_PIXELSSCANFUNCTION_HPP

#include "duckdb.hpp"
#include <fstream>
#include <iostream>
#include <numeric>
#include <string>
#include <vector>
#include <cstdio>
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "PixelsReadGlobalState.hpp"
#include "PixelsReadLocalState.hpp"
#include "PixelsReadBindData.hpp"

#include "physical/storage/LocalFS.h"
#include "physical/natives/ByteBuffer.h"
#include "physical/natives/DirectRandomAccessFile.h"
#include "physical/io/PhysicalLocalReader.h"
#include "physical/StorageFactory.h"
#include "PixelsReaderImpl.h"
#include "PixelsReaderBuilder.h"
#include <iostream>
#include <future>
#include <thread>
#include "physical/scheduler/NoopScheduler.h"
#include "physical/SchedulerFactory.h"
#include "PixelsVersion.h"
#include "PixelsFooterCache.h"
#include "exception/PixelsReaderException.h"
#include "reader/PixelsReaderOption.h"
#include "TypeDescription.h"
#include "vector/ColumnVector.h"
#include "vector/LongColumnVector.h"
#include "physical/BufferPool.h"
#include "profiler/TimeProfiler.h"
#include "physical/natives/DirectUringRandomAccessFile.h"
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/enums/file_compression_type.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/hive_partitioning.hpp"
#include "duckdb/common/union_by_name.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/multi_file_reader.hpp"
#endif

using namespace std;

namespace duckdb {

class PixelsScanFunction {
public:
	static TableFunctionSet GetFunctionSet();
	static void PixelsScanImplementation(ClientContext &context, TableFunctionInput &data_p,
	                                      DataChunk &output);
	static unique_ptr<FunctionData> PixelsScanBind(ClientContext &context, TableFunctionBindInput &input,
	                                                vector<LogicalType> &return_types, vector<string> &names);
	static unique_ptr<GlobalTableFunctionState> PixelsScanInitGlobal(ClientContext &context,
	                                                                  TableFunctionInitInput &input);
	static unique_ptr<LocalTableFunctionState>
	PixelsScanInitLocal(ExecutionContext &context, TableFunctionInitInput &input, GlobalTableFunctionState *gstate_p);
	static bool PixelsParallelStateNext(ClientContext &context, const PixelsReadBindData &bind_data,
	                                     PixelsReadLocalState &scan_data, PixelsReadGlobalState &parallel_state,
                                         bool is_init_state = false);
    static PixelsReaderOption GetPixelsReaderOption(PixelsReadLocalState &local_state, PixelsReadGlobalState &global_state);
private:
	static void TransformDuckdbType(const std::shared_ptr<TypeDescription>& type,
	                         vector<LogicalType> &return_types);
	static void TransformDuckdbChunk(PixelsReadLocalState & data,
	                            DataChunk &output,
	                            const std::shared_ptr<TypeDescription> & schema,
	                            unsigned long thisOutputChunkRows);
    static bool enable_filter_pushdown;
};

} // namespace duckdb
#endif // EXAMPLE_C_PIXELSSCANFUNCTION_HPP
