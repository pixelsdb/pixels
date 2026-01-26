/*
 * Copyright 2023 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */

/*
 * @author liyu
 * @create 2023-03-26
 */
#include "PixelsScanFunction.hpp"
#include "physical/StorageArrayScheduler.h"
#include "profiler/CountProfiler.h"

namespace duckdb
{

bool PixelsScanFunction::enable_filter_pushdown = true;

static idx_t PixelsScanGetBatchIndex(ClientContext &context, const FunctionData *bind_data_p,
                                     LocalTableFunctionState *local_state,
                                     GlobalTableFunctionState *global_state)
{
  auto &data = (PixelsReadLocalState &) *local_state;
  return data.curr_batch_index;
}

static double PixelsProgress(ClientContext &context, const FunctionData *bind_data_p,
                             const GlobalTableFunctionState *global_state)
{
  auto &bind_data = (PixelsReadBindData &) *bind_data_p;
  if (bind_data.files.empty())
    {
    return 100.0;
    }
  auto percentage = bind_data.curFileId * 100.0 / bind_data.files.size();
  return percentage;
}

static unique_ptr<NodeStatistics> PixelsCardinality(ClientContext &context, const FunctionData *bind_data)
{
  auto &data = (PixelsReadBindData &) *bind_data;

  return make_uniq<NodeStatistics>(data.initialPixelsReader->getNumberOfRows() * data.files.size());
}

TableFunctionSet PixelsScanFunction::GetFunctionSet()
{
  TableFunction table_function("pixels_scan", {LogicalType::VARCHAR}, PixelsScanImplementation, PixelsScanBind,
                               PixelsScanInitGlobal, PixelsScanInitLocal);
  table_function.projection_pushdown = true;
	table_function.filter_pushdown = true;
  // table_function.filter_prune = true;
  enable_filter_pushdown = table_function.filter_pushdown;
  MultiFileReader::AddParameters(table_function);
  table_function.cardinality = PixelsCardinality;
  table_function.table_scan_progress = PixelsProgress;
  // TODO: maybe we need other code here later. Refer parquet-extension.cpp
  return MultiFileReader::CreateFunctionSet(table_function);

}

void PixelsScanFunction::PixelsScanImplementation(ClientContext &context,
                                                  TableFunctionInput &data_p,
                                                  DataChunk &output)
{
  if (!data_p.local_state)
    {
    return;
    }

  auto &data = (PixelsReadLocalState &) *data_p.local_state;
  auto &gstate = (PixelsReadGlobalState &) *data_p.global_state;
  auto &bind_data = (PixelsReadBindData &) *data_p.bind_data;

  do
    {
    if (data.currPixelsRecordReader == nullptr ||
        (data.currPixelsRecordReader->isEndOfFile() && data.vectorizedRowBatch->isEndOfFile()))
      {
      if (data.currPixelsRecordReader != nullptr)
        {
        data.currPixelsRecordReader.reset();
        }
      if (!PixelsParallelStateNext(context, bind_data, data, gstate))
        {
        return;
        }
      }
    auto currPixelsRecordReader = std::static_pointer_cast<PixelsRecordReaderImpl>(data.currPixelsRecordReader);
    auto nextPixelsRecordReader = std::static_pointer_cast<PixelsRecordReaderImpl>(data.nextPixelsRecordReader);

    if (data.vectorizedRowBatch != nullptr && data.vectorizedRowBatch->isEndOfFile())
      {
      data.vectorizedRowBatch = nullptr;
      }
    if (data.vectorizedRowBatch == nullptr)
      {
      data.vectorizedRowBatch = currPixelsRecordReader->readBatch(false);
      }
    uint64_t currentLoc = data.vectorizedRowBatch->position();
    std::shared_ptr<TypeDescription> resultSchema = data.currPixelsRecordReader->getResultSchema();
    uint64_t remaining = data.vectorizedRowBatch->remaining();
    assert(remaining > 0);
    auto thisOutputChunkRows = MinValue<idx_t>(STANDARD_VECTOR_SIZE, remaining);
    output.SetCardinality(thisOutputChunkRows);
    std::shared_ptr<PixelsBitMask> filterMask =
        std::static_pointer_cast<PixelsRecordReaderImpl>(data.currPixelsRecordReader)->getFilterMask();

    TransformDuckdbChunk(data, output, resultSchema, thisOutputChunkRows);

    // apply the filter operation
    if (enable_filter_pushdown)
      {
      idx_t sel_size = 0;
      SelectionVector sel;
      sel.Initialize(thisOutputChunkRows);
      for (idx_t i = 0; i < thisOutputChunkRows; i++)
        {
        if (filterMask->get(i + currentLoc))
          {
          sel.set_index(sel_size++, i);
          }
        }
      output.Slice(sel, sel_size);
      }
    if (output.size() > 0)
      {
      return;
      } else
      {
      output.Reset();
      }
    } while (true);
}

struct compare_file_name
{
  inline bool operator()(const string &path1, const string &path2)
  {
    int num1 = filename2num(path1);
    int num2 = filename2num(path2);
    return num1 < num2;
  }

  // the pixels file name format is xxxxx_${number}.pxl. We transfer this name to ${number}
  static int filename2num(const string &filename)
  {
    string filename_without_suffix = filename.substr(0, filename.rfind('.'));
    int number = std::stoi(filename_without_suffix.substr(filename_without_suffix.rfind('_') + 1));
    return number;
  }
};

unique_ptr<FunctionData> PixelsScanFunction::PixelsScanBind(
    ClientContext &context, TableFunctionBindInput &input,
    vector<LogicalType> &return_types, vector<string> &names)
{
  if (input.inputs[0].IsNull())
    {
    throw ParserException("Pixels reader cannot take NULL list as parameter");
    }
  auto multi_file_reader = MultiFileReader::CreateDefault("PixelsScan");

  auto file_list = multi_file_reader->CreateFileList(context, input.inputs[0],
                                                     duckdb::FileGlobOptions::ALLOW_EMPTY);

  auto files = file_list->GetAllFiles();
  // parse *
  if (files.empty())
    {
    throw InvalidArgumentException("The number of pxl file should be positive. ");
    }
  vector<string> filePaths;
  for (auto file:files) {
    filePaths.push_back(file.path);
  }
  // sort the pxl file by file name, so that all SSD arrays can be fully utilized
  sort(filePaths.begin(), filePaths.end(), compare_file_name());

  auto footerCache = std::make_shared<PixelsFooterCache>();
  auto builder = std::make_shared<PixelsReaderBuilder>();

  std::shared_ptr<::Storage> storage = StorageFactory::getInstance()->getStorage(::Storage::file);
  std::shared_ptr<PixelsReader> pixelsReader = builder
      ->setPath(filePaths.at(0))
      ->setStorage(storage)
      ->setPixelsFooterCache(footerCache)
      ->build();
  std::shared_ptr<TypeDescription> fileSchema = pixelsReader->getFileSchema();
  TransformDuckdbType(fileSchema, return_types);
  names = fileSchema->getFieldNames();

  auto result = make_uniq<PixelsReadBindData>();
  result->initialPixelsReader = pixelsReader;
  result->fileSchema = fileSchema;
  result->files = filePaths;

  return std::move(result);
}

unique_ptr<GlobalTableFunctionState> PixelsScanFunction::PixelsScanInitGlobal(
    ClientContext &context, TableFunctionInitInput &input)
{

  auto &bind_data = (PixelsReadBindData &) *input.bind_data;

  auto result = make_uniq<PixelsReadGlobalState>();

  result->initialPixelsReader = bind_data.initialPixelsReader;

  int max_threads = std::stoi(ConfigFactory::Instance().getProperty("pixel.threads"));
  if (max_threads <= 0)
    {
    max_threads = (int) bind_data.files.size();
    }

  result->storageArrayScheduler = std::make_shared<StorageArrayScheduler>(bind_data.files, max_threads);

  result->file_index.resize(result->storageArrayScheduler->getDeviceSum());

  result->max_threads = max_threads;

  result->batch_index = 0;

  result->filters = input.filters.get();

  return std::move(result);
}

unique_ptr<LocalTableFunctionState> PixelsScanFunction::PixelsScanInitLocal(
    ExecutionContext &context, TableFunctionInitInput &input,
    GlobalTableFunctionState *gstate_p)
{
  auto &bind_data = (PixelsReadBindData &) *input.bind_data;

  auto &gstate = (PixelsReadGlobalState &) *gstate_p;

  auto result = make_uniq<PixelsReadLocalState>();

  result->deviceID = gstate.storageArrayScheduler->acquireDeviceId();

  result->column_ids = input.column_ids;

  auto fieldNames = bind_data.fileSchema->getFieldNames();

  for (column_t column_id : input.column_ids)
    {
    if (!IsRowIdColumnId(column_id))
      {
      result->column_names.emplace_back(fieldNames.at(column_id));
      }
    }

  ::DirectUringRandomAccessFile::Initialize();
  if (!PixelsParallelStateNext(context.client, bind_data, *result, gstate, true))
    {
    return nullptr;
    }
  return std::move(result);
}

void PixelsScanFunction::TransformDuckdbType(const std::shared_ptr<TypeDescription> &type,
                                             vector<LogicalType> &return_types)
{
  auto columnSchemas = type->getChildren();
  for (auto columnType : columnSchemas)
    {
    switch (columnType->getCategory())
      {
      //        case TypeDescription::BOOLEAN:
      //            break;
      //        case TypeDescription::BYTE:
      //            break;
      case TypeDescription::SHORT:
      case TypeDescription::INT:return_types.emplace_back(LogicalType::INTEGER);
        break;
      case TypeDescription::LONG:return_types.emplace_back(LogicalType::BIGINT);
        break;
        //        case TypeDescription::FLOAT:
        //            break;
        //        case TypeDescription::DOUBLE:
        //            break;
      case TypeDescription::DECIMAL:
        return_types.emplace_back(LogicalType::DECIMAL(columnType->getPrecision(),
                                                       columnType->getScale()));
        break;
      case TypeDescription::STRING:return_types.emplace_back(LogicalType::VARCHAR);
        break;
      case TypeDescription::DATE:return_types.emplace_back(LogicalType::DATE);
        break;
        //        case TypeDescription::TIME:
        //            break;
      case TypeDescription::TIMESTAMP:return_types.emplace_back(LogicalType::TIMESTAMP);
        break;
        //        case TypeDescription::VARBINARY:
        //            break;
        //        case TypeDescription::BINARY:
        //            break;
      case TypeDescription::VARCHAR:return_types.emplace_back(LogicalType::VARCHAR);
        break;
      case TypeDescription::CHAR:return_types.emplace_back(LogicalType::VARCHAR);
        break;
        //        case TypeDescription::STRUCT:
        //            break;
      default:
        throw InvalidArgumentException(
            "bad column type in TransformDuckdbType: " + std::to_string(type->getCategory()));
      }
    }
}

void PixelsScanFunction::TransformDuckdbChunk(PixelsReadLocalState &data,
                                              DataChunk &output,
                                              const std::shared_ptr<TypeDescription> &schema,
                                              uint64_t thisOutputChunkRows)
{
  int row_batch_id = 0;
  auto column_ids = data.column_ids;
  auto vectorizedRowBatch = data.vectorizedRowBatch;
  for (uint64_t col_id = 0; col_id < column_ids.size(); col_id++)
    {
    if (IsRowIdColumnId(column_ids.at(col_id)))
      {
      Value constant_42 = Value::BIGINT(42);
      output.data.at(col_id).Reference(constant_42);
      continue;
      }
    auto col = vectorizedRowBatch->cols.at(row_batch_id);
    auto colSchema = schema->getChildren().at(row_batch_id);
    switch (colSchema->getCategory())
      {
      //        case TypeDescription::BOOLEAN:
      //            break;
      //        case TypeDescription::BYTE:
      //            break;
      case TypeDescription::SHORT:
      case TypeDescription::INT:
        {
        auto intCol = std::static_pointer_cast<IntColumnVector>(col);
        Vector vector(LogicalType::INTEGER,
                      (data_ptr_t) (intCol->current()), col->currentValid(),col->getCapacity());
        output.data.at(col_id).Reference(vector);
//			    auto result_ptr = FlatVector::GetData<int>(output.data.at(col_id));
//			    memcpy(result_ptr, intCol->intVector + row_offset, thisOutputChunkRows * sizeof(int));
//			    for(long i = 0; i < thisOutputChunkRows; i++) {
//				    result_ptr[i] = intCol->intVector[i + row_offset];
//			    }

        break;
        }
      case TypeDescription::LONG:
        {
        auto longCol = std::static_pointer_cast<LongColumnVector>(col);
        Vector vector(LogicalType::BIGINT,
                      (data_ptr_t) (longCol->current()), col->currentValid(),col->getCapacity());
        output.data.at(col_id).Reference(vector);
//			    auto result_ptr = FlatVector::GetData<long>(output.data.at(col_id));
//			    memcpy(result_ptr, longCol->longVector + row_offset, thisOutputChunkRows * sizeof(long));
//			    for(long i = 0; i < thisOutputChunkRows; i++) {
//				    result_ptr[i] = longCol->longVector[i + row_offset];
//			    }
        break;
        }
        //        case TypeDescription::FLOAT:
        //            break;
        //        case TypeDescription::DOUBLE:
        //            break;
      case TypeDescription::DECIMAL:
        {
        auto decimalCol = std::static_pointer_cast<DecimalColumnVector>(col);
        Vector vector(LogicalType::DECIMAL(colSchema->getPrecision(), colSchema->getScale()),
                      (data_ptr_t) (decimalCol->current()), col->currentValid(),col->getCapacity());
        output.data.at(col_id).Reference(vector);
//			    auto result_ptr = FlatVector::GetData<long>(output.data.at(col_id));
//			    memcpy(result_ptr, decimalCol->vector + row_offset, thisOutputChunkRows * sizeof(long));
//			    for(long i = 0; i < thisOutputChunkRows; i++) {
//				    result_ptr[i] = decimalCol->vector[i + row_offset];
//			    }
        break;
        }

        //        case TypeDescription::STRING:
        //            break;
      case TypeDescription::DATE:
        {
        auto dateCol = std::static_pointer_cast<DateColumnVector>(col);
        Vector vector(LogicalType::DATE,
                      (data_ptr_t) (dateCol->current()), col->currentValid(),col->getCapacity());
        output.data.at(col_id).Reference(vector);
//			    auto result_ptr = FlatVector::GetData<int>(output.data.at(col_id));
//			    memcpy(result_ptr, dateCol->dates + row_offset, thisOutputChunkRows * sizeof(int));
//			    for(long i = 0; i < thisOutputChunkRows; i++) {
//				    result_ptr[i] = dateCol->dates[i + row_offset];
//			    }
        break;
        }

        //        case TypeDescription::TIME:
        //            break;
      case TypeDescription::TIMESTAMP:
        {
        auto tsCol = std::static_pointer_cast<TimestampColumnVector>(col);
        Vector vector(LogicalType::TIMESTAMP,
                      (data_ptr_t) (tsCol->current()), col->currentValid(),col->getCapacity());
        output.data.at(col_id).Reference(vector);
        break;
        }

        //        case TypeDescription::VARBINARY:
        //            break;
        //        case TypeDescription::BINARY:
        //            break;
      case TypeDescription::VARCHAR:
      case TypeDescription::CHAR:
      case TypeDescription::STRING:
        {
        auto binaryCol = std::static_pointer_cast<BinaryColumnVector>(col);
        Vector vector(LogicalType::VARCHAR,
                      (data_ptr_t) (binaryCol->current()), col->currentValid(),col->getCapacity());
        output.data.at(col_id).Reference(vector);
//			    auto result_ptr = FlatVector::GetData<duckdb::string_t>(output.data.at(col_id));
//                memcpy(result_ptr, binaryCol->vector + row_offset, thisOutputChunkRows * sizeof(string_t));
        break;
        }
        //        case TypeDescription::STRUCT:
        //            break;
//			default:
//				throw InvalidArgumentException("bad column type " + std::to_string(colSchema->getCategory()));
      }
    row_batch_id++;
    }
  vectorizedRowBatch->increment(thisOutputChunkRows);
}

bool PixelsScanFunction::PixelsParallelStateNext(ClientContext &context, PixelsReadBindData &bind_data,
                                                 PixelsReadLocalState &scan_data,
                                                 PixelsReadGlobalState &parallel_state,
                                                 bool is_init_state)
{
  unique_lock<mutex> parallel_lock(parallel_state.lock);
  if (parallel_state.error_opening_file)
    {
    throw InvalidArgumentException("PixelsScanInitLocal: file open error.");
    }

  auto &StorageInstance = parallel_state.storageArrayScheduler;
  // In the following two cases, the state ends:
  // 1. When PixelsScanInitLocal invokes this function, if all files are
  // fetched by other threads, this means this thread doesn't need do anything, so just return false;
  // 2. When PixelsScanImplementation invokes this function (scan_data.next_file_index > -1), if
  // scan_data.next_file_index >= (int) StorageInstance.getFileSum(scan_data.deviceID), it means the current file is already
  // done, so the function return false.
  if ((is_init_state &&
      parallel_state.file_index.at(scan_data.deviceID) >= StorageInstance->getFileSum(scan_data.deviceID)) ||
      scan_data.next_file_index >= StorageInstance->getFileSum(scan_data.deviceID))
    {
    ::BufferPool::Reset();
    // if async io is enabled, we need to unregister uring buffer
    if (ConfigFactory::Instance().boolCheckProperty("localfs.enable.async.io"))
      {
      if (ConfigFactory::Instance().getProperty("localfs.async.lib") == "iouring")
        {
        ::DirectUringRandomAccessFile::Reset();
        ::TimeProfiler::Instance().Print();
        } else if (ConfigFactory::Instance().getProperty("localfs.async.lib") == "aio")
        {
        throw InvalidArgumentException(
            "PhysicalLocalReader::readAsync: We don't support aio for our async read yet.");
        }
      }
    parallel_lock.unlock();
    return false;
    }
  bind_data.curFileId++;
  scan_data.curr_file_index = scan_data.next_file_index;
  scan_data.curr_batch_index = scan_data.next_batch_index;
  scan_data.next_file_index = parallel_state.file_index.at(scan_data.deviceID);
  scan_data.next_batch_index = StorageInstance->getBatchID(scan_data.deviceID, scan_data.next_file_index);
  scan_data.curr_file_name = scan_data.next_file_name;
  parallel_state.file_index.at(scan_data.deviceID)++;
  parallel_lock.unlock();
  // The below code uses global state but no race happens, so we don't need the lock anymore


  if (scan_data.currReader != nullptr)
    {
    scan_data.currReader->close();
    }

  if (ConfigFactory::Instance().getProperty("pixels.doublebuffer")=="true")
  {
    ::BufferPool::Switch();
  }
  // double/single buffer

  scan_data.currReader = scan_data.nextReader;
  scan_data.currPixelsRecordReader = scan_data.nextPixelsRecordReader;
  // asyncReadComplete is not invoked in the first run (is_init_state = true)
  if (scan_data.currPixelsRecordReader != nullptr)
    {
    auto currPixelsRecordReader = std::static_pointer_cast<PixelsRecordReaderImpl>(
        scan_data.currPixelsRecordReader);
    if (ConfigFactory::Instance().getProperty("pixels.doublebuffer")=="false")
    {
      //single buffer
      currPixelsRecordReader->read();
    }

    currPixelsRecordReader->asyncReadComplete((int) scan_data.column_names.size());
    }
  if (scan_data.next_file_index < StorageInstance->getFileSum(scan_data.deviceID))
    {
    auto footerCache = std::make_shared<PixelsFooterCache>();
    auto builder = std::make_shared<PixelsReaderBuilder>();
    std::shared_ptr<::Storage> storage = StorageFactory::getInstance()->getStorage(::Storage::file);
    scan_data.next_file_name = StorageInstance->getFileName(scan_data.deviceID, scan_data.next_file_index);
    scan_data.nextReader = builder->setPath(scan_data.next_file_name)
        ->setStorage(storage)
        ->setPixelsFooterCache(footerCache)
        ->build();

    PixelsReaderOption option = GetPixelsReaderOption(scan_data, parallel_state);
    scan_data.nextPixelsRecordReader = scan_data.nextReader->read(option);
    auto nextPixelsRecordReader = std::static_pointer_cast<PixelsRecordReaderImpl>(
        scan_data.nextPixelsRecordReader);

    if (ConfigFactory::Instance().getProperty("pixels.doublebuffer")=="true")
    {
      //double buffer
      nextPixelsRecordReader->read();
    }

    } else
    {
    scan_data.nextReader = nullptr;
    scan_data.nextPixelsRecordReader = nullptr;
    }
  return true;
}

PixelsReaderOption
PixelsScanFunction::GetPixelsReaderOption(PixelsReadLocalState &local_state, PixelsReadGlobalState &global_state)
{
  PixelsReaderOption option;
  option.setSkipCorruptRecords(true);
  option.setTolerantSchemaEvolution(true);
  option.setEnableEncodedColumnVector(true);
  option.setFilter(global_state.filters);
  option.setEnabledFilterPushDown(enable_filter_pushdown);
  // includeCols comes from the caller of PixelsPageSource
  option.setIncludeCols(local_state.column_names);
  option.setRGRange(0, local_state.nextReader->getRowGroupNum());
  option.setQueryId(1);
  int stride = std::stoi(ConfigFactory::Instance().getProperty("pixel.stride"));
  option.setBatchSize(stride);
  return option;
}
}
