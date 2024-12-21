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
 * @create 2023-03-06
 */
#include "PixelsReaderImpl.h"

PixelsReaderImpl::PixelsReaderImpl(std::shared_ptr<TypeDescription> fileSchema,
                                   std::shared_ptr<PhysicalReader> reader,
                                   std::shared_ptr<pixels::proto::FileTail> fileTail,
                                   std::shared_ptr<PixelsFooterCache> footerCache) {
	this->fileSchema = fileSchema;
	this->physicalReader = reader;
	this->footer = fileTail->footer();
	this->postScript = fileTail->postscript();
	this->pixelsFooterCache = footerCache;
	this->closed = false;
}


/**
 * Prepare for the next row batch. This method is independent from readBatch().
 *
 * @param batchSize the willing batch size
 * @return the real batch size
 */
std::shared_ptr<PixelsRecordReader> PixelsReaderImpl::read(PixelsReaderOption option) {
    // TODO: add a function parameter, and the code before creating PixelsRecordReaderImpl
	std::shared_ptr<PixelsRecordReader> recordReader =
	    std::make_shared<PixelsRecordReaderImpl>(
            physicalReader, postScript,
            footer, option, pixelsFooterCache);
    recordReaders.emplace_back(recordReader);
    return recordReader;
}
std::shared_ptr<TypeDescription> PixelsReaderImpl::getFileSchema() {
	return fileSchema;
}

PixelsVersion::Version PixelsReaderImpl::getFileVersion() {
	return PixelsVersion::from(postScript.version());
}

long PixelsReaderImpl::getNumberOfRows() {
	return postScript.numberofrows();
}

pixels::proto::CompressionKind PixelsReaderImpl::getCompressionKind() {
	return postScript.compression();
}

long PixelsReaderImpl::getCompressionBlockSize() {
	return postScript.compressionblocksize();
}

long PixelsReaderImpl::getPixelStride() {
	return postScript.pixelstride();
}

std::string PixelsReaderImpl::getWriterTimeZone() {
	return postScript.writertimezone();
}

int PixelsReaderImpl::getRowGroupNum() {
	return footer.rowgroupinfos_size();
}

bool PixelsReaderImpl::isPartitioned() {
	return postScript.has_partitioned() && postScript.partitioned();
}

ColumnStatisticList PixelsReaderImpl::getColumnStats() {
	return footer.columnstats();
}

pixels::proto::ColumnStatistic PixelsReaderImpl::getColumnStat(std::string columnName) {
	auto fieldNames = fileSchema->getFieldNames();
	auto fieldIter = std::find(fieldNames.begin(), fieldNames.end(), columnName);
	if(fieldIter == fieldNames.end()) {
		throw InvalidArgumentException("the column " +
										 columnName + " is not the field name!");
	}
	int fieldId = fieldIter - fieldNames.begin();
	return footer.columnstats().Get(fieldId);
}

RowGroupInfoList PixelsReaderImpl::getRowGroupInfos() {
	return footer.rowgroupinfos();
}

pixels::proto::RowGroupInformation PixelsReaderImpl::getRowGroupInfo(int rowGroupId) {
	if(rowGroupId < 0 || rowGroupId >= footer.columnstats_size()) {
		throw InvalidArgumentException("row group id is out of bound.");
	}
	return footer.rowgroupinfos().Get(rowGroupId);
}

pixels::proto::RowGroupStatistic PixelsReaderImpl::getRowGroupStat(int rowGroupId) {
	if(rowGroupId < 0 || rowGroupId >= footer.columnstats_size()) {
		throw InvalidArgumentException("row group id is out of bound.");
	}
	return footer.rowgroupstats().Get(rowGroupId);
}

RowGroupStatList PixelsReaderImpl::getRowGroupStats() {
	return footer.rowgroupstats();
}

PixelsReaderImpl::~PixelsReaderImpl() {
	if(!closed) {
		PixelsReaderImpl::close();
	}
}

void PixelsReaderImpl::close() {
	if(!closed) {
		for(auto recordReader: recordReaders) {
			recordReader->close();
		}
		recordReaders.clear();
		physicalReader->close();
	}
}
