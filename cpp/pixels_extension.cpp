//
// Created by liyu on 3/26/23.
//
#define DUCKDB_EXTENSION_MAIN

#include "pixels_extension.hpp"
#include "PixelsScanFunction.hpp"
#include "PixelsReadBindData.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

namespace duckdb {

    // Pixels Scan Replacemet for duckdb 1.0
unique_ptr<TableRef> PixelsScanReplacement(ClientContext &context,ReplacementScanInput &input,
                                               optional_ptr<ReplacementScanData> data){
    auto table_name=ReplacementScan::GetFullPath(input);
//    if(!ReplacementScan::CanReplace(table_name,{"pixels"})){
//        return nullptr;
//    }
    auto lower_name = StringUtil::Lower(table_name);
	if (!StringUtil::EndsWith(lower_name, ".pxl") && !StringUtil::Contains(lower_name, ".pxl?")) {
		return nullptr;
	}
    auto table_function= make_uniq<TableFunctionRef>();
    vector<unique_ptr<ParsedExpression>> children;
    children.push_back(make_uniq<ConstantExpression>(Value(table_name)));
    table_function->function = make_uniq<FunctionExpression>("pixels_scan",std::move(children));
    if(!FileSystem::HasGlob(table_name)){
        auto &fs=FileSystem::GetFileSystem(context);
        table_function->alias=fs.ExtractBaseName(table_name);
    }
    return std::move(table_function);
}

void PixelsExtension::Load(DuckDB &db) {
	Connection con(*db.instance);
	con.BeginTransaction();

	auto &context = *con.context;
	auto &catalog = Catalog::GetSystemCatalog(*con.context);

	auto scan_fun = PixelsScanFunction::GetFunctionSet();
	CreateTableFunctionInfo cinfo(scan_fun);
	cinfo.name = "pixels_scan";

	catalog.CreateTableFunction(context, &cinfo);
	con.Commit();

	auto &config = DBConfig::GetConfig(*db.instance);
	config.replacement_scans.emplace_back(PixelsScanReplacement);
}

std::string PixelsExtension::Name() {
	return "pixels";
}


} // namespace duckdb

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif

