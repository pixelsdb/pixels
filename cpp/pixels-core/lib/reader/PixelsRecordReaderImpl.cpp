//
// Created by liyu on 3/7/23.
//

#include "reader/PixelsRecordReaderImpl.h"
#include "physical/io/PhysicalLocalReader.h"
#include "profiler/CountProfiler.h"

PixelsRecordReaderImpl::PixelsRecordReaderImpl(std::shared_ptr<PhysicalReader> reader,
                                               const pixels::proto::PostScript& pixelsPostScript,
                                               const pixels::proto::Footer& pixelsFooter,
                                               const PixelsReaderOption& opt,
                                               std::shared_ptr<PixelsFooterCache> pixelsFooterCache) {
    physicalReader = reader;
    footer = pixelsFooter;
    postScript = pixelsPostScript;
    footerCache = pixelsFooterCache;
    option = opt;
    // TODO: intialize all kinds of variables
    queryId = option.getQueryId();
    RGStart = option.getRGStart();
    RGLen = option.getRGLen();
    batchSize = option.getBatchSize();
    // batchSize must be larger than STANDARD_VECTOR_SIZE
    assert(batchSize >= STANDARD_VECTOR_SIZE);
    enabledFilterPushDown = option.isEnabledFilterPushDown();
    if(enabledFilterPushDown) {
        filter = option.getFilter();
    } else {
        filter = nullptr;
    }
    filterMask = nullptr;
    everRead = false;
	everPrepareRead = false;
    targetRGNum = 0;
    curRGIdx = 0;
    curRowInRG = 0;
	curRGRowCount = 0;
    fileName = physicalReader->getName();
    enableEncodedVector = option.isEnableEncodedColumnVector();
    includedColumnNum = 0;
	endOfFile = false;
    resultRowBatch = nullptr;

    checkBeforeRead();
}

void PixelsRecordReaderImpl::checkBeforeRead() {
    // get file schema
    auto fileColTypesFooterTypes = footer.types();
    auto fileColTypes = std::vector<std::shared_ptr<pixels::proto::Type>>{};
    for(const auto& type : fileColTypesFooterTypes) {
        fileColTypes.emplace_back(std::make_shared<::pixels::proto::Type>(type));
    }
    // TODO: if fileCOlTypes == null
    fileSchema = TypeDescription::createSchema(fileColTypes);
    // TODO: getChildren == NULL
    // filter included columns
    includedColumnNum = 0;
    auto optionIncludedCols = option.getIncludedCols();
    // TODO: if size of cols is 0, create an empty row batch
    // TODO: what if false is caused? we must debug this! Currently I didn't understand why we need includedColumns yet. So just leave it alone.
    includedColumns.clear();
    includedColumns.resize(fileColTypes.size());
    std::vector<int> optionColsIndices;
    for(const auto& col: optionIncludedCols) {
        for(int j = 0; j < fileColTypes.size(); j ++) {
            if(icompare(col, fileColTypes.at(j)->name())) {
                optionColsIndices.emplace_back(j);
                includedColumns.at(j) = true;
                includedColumnNum++;
                break;
            }
        }

    }
    // TODO: check includedColumns
    // create result columns storing result column ids in user specified order
    resultColumns.clear();
    resultColumns.resize(includedColumnNum);
    for(int i = 0; i < includedColumnNum; i++) {
        resultColumns.at(i) = optionColsIndices[i];
    }


    auto optionColsIndicesSet = std::set<int>(
            optionColsIndices.begin(), optionColsIndices.end());
    int targetColumnNum = (int)optionColsIndicesSet.size();
    targetColumns.clear();
    targetColumns.resize(targetColumnNum);
    int targetColIdx = 0;
    for(int i = 0; i < includedColumns.size(); i++) {
        if(includedColumns[i]) {
            targetColumns.at(targetColIdx) = i;
            targetColIdx++;
        }
    }

    // create column readers
    auto columnSchemas = fileSchema->getChildren();
    readers.clear();
    readers.resize(resultColumns.size());
    for(int i = 0; i < resultColumns.size(); i++) {
        int index = resultColumns[i];
        readers.at(i) = ColumnReaderBuilder::newColumnReader(columnSchemas.at(index));
    }

    // create result vectorized row batch
    for(int resultColumn: resultColumns) {
        includedColumnTypes.emplace_back(fileColTypes.at(resultColumn));
    }
    resultSchema = TypeDescription::createSchema(includedColumnTypes);

}


void PixelsRecordReaderImpl::UpdateRowGroupInfo() {
	// if not end of file, update row count
	curRGRowCount = (int) footer.rowgroupinfos(targetRGs.at(curRGIdx)).numberofrows();

    if(enabledFilterPushDown) {
        int length = std::min(batchSize, curRGRowCount);
        filterMask = std::make_shared<PixelsBitMask>(length);
    }

	curRGFooter = rowGroupFooters.at(curRGIdx);
	// refresh resultColumnsEncoded for reading the column vectors in the next row group.
	const pixels::proto::RowGroupEncoding& rgEncoding = rowGroupFooters.at(curRGIdx)->rowgroupencoding();
	for(int i = 0; i < includedColumnNum; i++) {
		resultColumnsEncoded.at(i) =
		    rgEncoding.columnchunkencodings(resultColumns.at(i))
		            .kind() != pixels::proto::ColumnEncoding_Kind_NONE
		    && enableEncodedVector;
	}
	for(int i = 0; i < resultColumns.size(); i++) {
		curEncoding.at(i) = std::make_shared<pixels::proto::ColumnEncoding>(rgEncoding.columnchunkencodings(resultColumns.at(i)));
		curChunkBufferIndex.at(i) = resultColumns.at(i);
		curChunkIndex.at(i) = std::make_shared<pixels::proto::ColumnChunkIndex>(curRGFooter->rowgroupindexentry()
		                          .columnchunkindexentries(resultColumns.at(i)));
	}
	// This flag makes sure that each row group invokes read()
	everRead = false;
}




// If cross multiple row group, we only process one row group
std::shared_ptr<VectorizedRowBatch> PixelsRecordReaderImpl::readBatch(bool reuse) {
    if(endOfFile) {
		endOfFile = true;
		return createEmptyEOFRowBatch(0);
	}
	if(!everRead) {
		if(!read()) {
			throw std::runtime_error("failed to read file");
		}
	}


	// TODO: resultRowBatch.projectionSize


    // update current batch size
    int curBatchSize = std::min(curRGRowCount - curRowInRG, std::min(batchSize, curRGRowCount));
    if(resultRowBatch == nullptr) {
        resultRowBatch = resultSchema->createRowBatch(curBatchSize, resultColumnsEncoded);
    } else {
        resultRowBatch->reset();
        if(curBatchSize != resultRowBatch->maxSize) {
            resultRowBatch->resize(curBatchSize);
        }
    }

    auto columnVectors = resultRowBatch->cols;
    if(filterMask != nullptr) {
        filterMask->set();
    }

    std::vector<int> filterColumnIndex;
    if(filter != nullptr) {
        for (auto &filterCol : filter->filters) {
            if(filterMask->isNone()) {
                break;
            }
            int i = filterCol.first;
            int index = curChunkBufferIndex.at(i);
            auto & encoding = curEncoding.at(i);
            auto & chunkIndex = curChunkIndex.at(i);
            readers.at(i)->read(chunkBuffers.at(index), *encoding, curRowInRG, curBatchSize,
                                postScript.pixelstride(), resultRowBatch->rowCount,
                                columnVectors.at(i), *chunkIndex, filterMask);
            filterColumnIndex.emplace_back(index);
            PixelsFilter::ApplyFilter(columnVectors.at(i), *filterCol.second, *filterMask,
                                      resultSchema->getChildren().at(i));
        }
    }

    // read vectors
    for(int i = 0; i < resultColumns.size(); i++) {
        // TODO: Refer to Issue #564. Disable data skipping
        //if(filterMask != nullptr) {
        //    if(filterMask->isNone()) {
        //        break;
        //    }
        //}
        // Skip the columns that calculate the filter mask, since they are already processed
        int index = curChunkBufferIndex.at(i);
        if(std::find(filterColumnIndex.begin(), filterColumnIndex.end(), index) != filterColumnIndex.end()) {
            continue;
        }
        auto & encoding = curEncoding.at(i);
        auto & chunkIndex = curChunkIndex.at(i);
        readers.at(i)->read(chunkBuffers.at(index), *encoding, curRowInRG, curBatchSize,
                            postScript.pixelstride(), resultRowBatch->rowCount,
                            columnVectors.at(i), *chunkIndex, filterMask);
    }

    // update current row index in the row group
    curRowInRG += curBatchSize;
    resultRowBatch->rowCount += curBatchSize;
    // update row group index if current row index exceeds max row count in the row group
    if(curRowInRG >= curRGRowCount) {
        curRGIdx++;
        if(curRGIdx < targetRGNum) {
            UpdateRowGroupInfo();
        } else {
            // if end of file, set result vectorized row batch endOfFile
            // TODO: set checkValid to false!
            endOfFile = true;
        }
        curRowInRG = 0;
    }
	return resultRowBatch;
}


void PixelsRecordReaderImpl::prepareRead() {
	everPrepareRead = true;
    std::vector<bool> includedRGs;
    includedRGs.resize(RGLen);

    uint64_t includedRowNum = 0;
    // read row group statistics and find target row groups
    for(int i = 0; i < RGLen; i++) {
        includedRGs.at(i) = true;
        includedRowNum += footer.rowgroupinfos(RGStart + i).numberofrows();
    }
    targetRGs.clear();
    targetRGs.resize(RGLen);
    int targetRGIdx = 0;
    for(int i = 0; i < RGLen; i++) {
        if(includedRGs[i]) {
            targetRGs.at(targetRGIdx) = i + RGStart;
            targetRGIdx++;
        }
    }
    targetRGNum = targetRGIdx;

    // TODO: if taregetRGNum == 0

    // read row group footers
    rowGroupFooters.clear();
    rowGroupFooters.resize(targetRGNum);
    std::vector<bool> rowGroupFooterCacheHit;
    rowGroupFooterCacheHit.resize(targetRGNum);

    /**
     * Issue #114:
     * Use request batch and read scheduler to execute the read requests.
     *
     * Here, we create an empty batch as footer cache is very likely to be hit in
     * the subsequent queries on the same table.
     */
    RequestBatch requestBatch;
    std::vector<int> fis;
    std::vector<std::string> rgCacheIds;
    for(int i = 0; i < targetRGNum; i++) {
        int rgId = targetRGs[i];
        std::string rgCacheId = fileName + "-" + std::to_string(rgId);
        rgCacheIds.emplace_back(rgCacheId);
        if(footerCache != nullptr && footerCache->containsRGFooter(rgCacheId)) {
            // cache hit
            rowGroupFooters.at(i) = footerCache->getRGFooter(rgCacheId);
            rowGroupFooterCacheHit.at(i) = true;
        } else {
            // cache miss, read from disk and put it into cache
            const pixels::proto::RowGroupInformation& rowGroupInformation = footer.rowgroupinfos(rgId);
            uint64_t footerOffset = rowGroupInformation.footeroffset();
            uint64_t footerLength = rowGroupInformation.footerlength();
            fis.push_back(i);
            requestBatch.add(queryId, (int) footerOffset, (int) footerLength);
            rowGroupFooterCacheHit.at(i) = false;
        }
    }
    Scheduler * scheduler = SchedulerFactory::Instance()->getScheduler();
    auto bbs = scheduler->executeBatch(physicalReader, requestBatch, queryId);
    // TODO: the return value should be unique_ptr?

    for(int i = 0; i < bbs.size(); i++) {
        if(!rowGroupFooterCacheHit.at(i)) {
			auto parsed = std::make_shared<pixels::proto::RowGroupFooter>();
            parsed->ParseFromArray(bbs[i]->getPointer(), (int)bbs[i]->size());
            rowGroupFooters.at(fis[i]) = parsed;
			if(footerCache != nullptr) {
				footerCache->putRGFooter(rgCacheIds[fis[i]], parsed);
			}
        }
    }

    bbs.clear();
    resultColumnsEncoded.clear();
    resultColumnsEncoded.resize(includedColumnNum);

	curEncoding.resize(resultColumns.size());
	curChunkBufferIndex.resize(resultColumns.size());
	curChunkIndex.resize(resultColumns.size());
	UpdateRowGroupInfo();
}

void PixelsRecordReaderImpl::asyncReadComplete(int requestSize) {
    if(ConfigFactory::Instance().boolCheckProperty("localfs.enable.async.io")) {
        if(ConfigFactory::Instance().getProperty("localfs.async.lib") == "iouring") {
            auto localReader = std::static_pointer_cast<PhysicalLocalReader>(physicalReader);
            localReader->readAsyncComplete(requestSize);
        } else if(ConfigFactory::Instance().getProperty("localfs.async.lib") == "aio") {
            throw InvalidArgumentException("PhysicalLocalReader::readAsync: We don't support aio for our async read yet.");
        }
    }

}


std::shared_ptr<PixelsBitMask> PixelsRecordReaderImpl::getFilterMask() {
    return filterMask;
}

bool PixelsRecordReaderImpl::read() {
	if(!everPrepareRead) {
		prepareRead();
	}

    everRead = true;

    // read chunk offset and length of each target column chunks

    // TODO: this should remove later
    chunkBuffers.clear();
    chunkBuffers.resize(includedColumns.size());
    std::vector<ChunkId> diskChunks;
    diskChunks.reserve(targetColumns.size());


    // TODO: support cache read

	const pixels::proto::RowGroupIndex& rowGroupIndex =
			rowGroupFooters[curRGIdx]->rowgroupindexentry();
	for(int colId: targetColumns) {
		const pixels::proto::ColumnChunkIndex& chunkIndex =
				rowGroupIndex.columnchunkindexentries(colId);
        if (!chunkIndex.littleendian()) {
            throw InvalidArgumentException("Pixels C++ reader only supports little endianness. ");
        }
		ChunkId chunk(curRGIdx, colId, chunkIndex.chunkoffset(), chunkIndex.chunklength());
		diskChunks.emplace_back(chunk);
	}


    if(!diskChunks.empty()) {
        RequestBatch requestBatch((int)diskChunks.size());
        Scheduler * scheduler = SchedulerFactory::Instance()->getScheduler();
		std::vector<uint32_t> colIds;
		std::vector<uint64_t> bytes;
        for(int i = 0; i < diskChunks.size(); i++) {
            ChunkId chunk = diskChunks.at(i);
            requestBatch.add(queryId, chunk.offset, (int)chunk.length, ::BufferPool::GetBufferId(i));
			colIds.emplace_back(chunk.columnId);
			bytes.emplace_back(chunk.length);
        }
		::BufferPool::Initialize(colIds, bytes);
        ::DirectUringRandomAccessFile::RegisterBufferFromPool(colIds);
		std::vector<std::shared_ptr<ByteBuffer>> originalByteBuffers;
		for(int i = 0; i < colIds.size(); i++) {
            auto colId = colIds.at(i);
			originalByteBuffers.emplace_back(::BufferPool::GetBuffer(colId));
		}

		auto byteBuffers = scheduler->executeBatch(physicalReader, requestBatch, originalByteBuffers, queryId);

        for(int index = 0; index < diskChunks.size(); index++) {
            ChunkId chunk = diskChunks.at(index);
            std::shared_ptr<ByteBuffer> bb = byteBuffers.at(index);
            uint32_t colId = chunk.columnId;
            if(bb != nullptr) {
                chunkBuffers.at(colId) = bb;
            }
        }
    }
    return true;

}

PixelsRecordReaderImpl::~PixelsRecordReaderImpl() {
    // TODO: chunkBuffers, physicalReader should be deleted?
}

std::shared_ptr<TypeDescription> PixelsRecordReaderImpl::getResultSchema() {
	return resultSchema;
}

/**
     * Create a row batch without any data, only sets the number of rows (size) and OEF.
     * Such a row batch is used for queries such as select count(*).
     * @param size the number of rows in the row batch.
     * @return the empty row batch.
 */
std::shared_ptr<VectorizedRowBatch> PixelsRecordReaderImpl::createEmptyEOFRowBatch(int size) {
	auto emptySchema = TypeDescription::createSchema(
	    std::vector<std::shared_ptr<pixels::proto::Type>>());
	auto emptyRowBatch = emptySchema->createRowBatch(0);
	emptyRowBatch->rowCount = 0;
	return emptyRowBatch;
}
bool PixelsRecordReaderImpl::isEndOfFile() {
	return endOfFile;
}

void PixelsRecordReaderImpl::close() {
	// release chunk buffers
	chunkBuffers.clear();
	for(const auto& reader: readers) {
		reader->close();
	}
    if (resultRowBatch != nullptr) {
        resultRowBatch->close();
    }
	readers.clear();
	rowGroupFooters.clear();
	includedColumnTypes.clear();
	endOfFile = true;
}

