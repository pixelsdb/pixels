//
// Created by liyu on 5/2/23.
//
#include <gtest/gtest.h>
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
#include <malloc.h>
#include "utils/ConfigFactory.h"
#include "physical/natives/DirectIoLib.h"
#include "physical/MergedRequest.h"
#include "physical/scheduler/SortMergeScheduler.h"
#include <pthread.h>
#include <glob.h>
#include <chrono>

using namespace std;

struct PixelsReadLocalState {
	shared_ptr<PixelsRecordReader> pixelsRecordReader;
	// this is used for storing row batch results.
	vector<shared_ptr<VectorizedRowBatch>> vectorizedRowBatchs;
	vector<int> column_ids;
	vector<string> column_names;
	shared_ptr<PixelsReader> reader;
	int file_index;
	string file_name;
	long rowCount;
};

struct PixelsReadGlobalState {
	mutex lock;
	std::vector<string> files;
	//! Currently opened readers
	vector<shared_ptr<PixelsReader>> readers;
	//! Index of file currently up for scanning
	int file_index;
	int max_threads;
	long rowCount;
};

PixelsReadGlobalState global;

vector<string> globVector(const string& pattern){
	glob_t glob_result;
	glob(pattern.c_str(),GLOB_TILDE,NULL,&glob_result);
	vector<string> files;
	for(unsigned int i=0;i<glob_result.gl_pathc;++i){
		files.push_back(string(glob_result.gl_pathv[i]));
	}
	globfree(&glob_result);
	return files;
}

bool UpdateLocalState(PixelsReadLocalState & local) {
	unique_lock<mutex> parallel_lock(global.lock);
	global.rowCount += local.rowCount;
	local.rowCount = 0;
	if(global.file_index >= global.files.size()) {
		return false;
	} else {
		local.file_index = global.file_index;
		local.file_name = global.files.at(local.file_index);
		global.file_index++;
		return true;
	}
}

void ScanImplementation() {
	PixelsReadLocalState local;
	local.rowCount = 0;
	while(true) {
		if(!UpdateLocalState(local)) {
			break;
		}
		auto footerCache = std::make_shared<PixelsFooterCache>();
		auto builder = std::make_shared<PixelsReaderBuilder>();
		auto storage = StorageFactory::getInstance()->getStorage(Storage::file);
		auto pixelsReader = builder
		                        ->setPath(local.file_name)
		                        ->setStorage(storage)
		                        ->setPixelsFooterCache(footerCache)
		                        ->build();

		PixelsReaderOption option;
		option.setSkipCorruptRecords(true);
		option.setTolerantSchemaEvolution(true);
		option.setEnableEncodedColumnVector(true);

		// includeCols comes from the caller of PixelsPageSource
		std::vector<std::string> includeCols = pixelsReader->getFileSchema()->getFieldNames();
		option.setIncludeCols(includeCols);
		option.setRGRange(0, 1);
		option.setQueryId(1);
		auto pixelsRecordReader = pixelsReader->read(option);
		while(true) {
			std::shared_ptr<VectorizedRowBatch> v = pixelsRecordReader->readBatch(false);
			local.rowCount += v->rowCount;
			if(v->endOfFile) {
				break;
			}
		}

	}

}

#define MAX_THREAD 1
TEST(reader, MultiThreadIOTest) {
	// collect all files
	std::string path = "/data/s1725-1/liyu/pixels_data/pixels-tpch-300-big-endian/lineitem/v-0-order/*.pxl";
	auto files = globVector(path);
	global.files = files;
	global.file_index = 0;
	global.rowCount = 0;
	thread threads [MAX_THREAD] = {};
	std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
	for(int i = 0; i < MAX_THREAD; i++) {
		threads[i] = thread(ScanImplementation);
	}
	for(int i = 0; i < MAX_THREAD; i++) {
		threads[i].join();
	}
	std::cout<<global.rowCount<<std::endl;
	std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
	std::cout << "Elapsed time = " << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count() << "[ms]" << std::endl;
}
