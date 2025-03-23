//
// Created by liyu on 3/6/23.
//

#ifndef PIXELS_PIXELSREADERIMPL_H
#define PIXELS_PIXELSREADERIMPL_H

#include "PixelsReader.h"
#include "reader/PixelsRecordReaderImpl.h"
#include <iostream>
#include <vector>
#include "pixels-common/pixels.pb.h"
#include "PixelsFooterCache.h"
#include "reader/PixelsReaderOption.h"


class PixelsReaderBuilder;

class PixelsReaderImpl: public PixelsReader {
public:
    std::shared_ptr<PixelsRecordReader> read(PixelsReaderOption option);
	PixelsReaderImpl(std::shared_ptr<TypeDescription> fileSchema,
	                 std::shared_ptr<PhysicalReader> reader,
	                 std::shared_ptr<pixels::proto::FileTail> fileTail,
	                 std::shared_ptr<PixelsFooterCache> footerCache);
	~PixelsReaderImpl();
	std::shared_ptr<TypeDescription> getFileSchema() override;
	PixelsVersion::Version getFileVersion() override;
	long getNumberOfRows() override;
	pixels::proto::CompressionKind getCompressionKind() override;
	long getCompressionBlockSize() override;
	long getPixelStride() override;
	std::string getWriterTimeZone() override;
	int getRowGroupNum() override;
	bool isPartitioned() override;
	ColumnStatisticList getColumnStats() override;
	pixels::proto::ColumnStatistic getColumnStat(std::string columnName) override;
	RowGroupInfoList getRowGroupInfos() override;
	pixels::proto::RowGroupInformation getRowGroupInfo(int rowGroupId) override;
	pixels::proto::RowGroupStatistic getRowGroupStat(int rowGroupId) override;
	RowGroupStatList getRowGroupStats() override;
	void close() override;
private:
    std::vector<std::shared_ptr<PixelsRecordReader>> recordReaders;
	std::shared_ptr<TypeDescription> fileSchema;
    std::shared_ptr<PhysicalReader> physicalReader;
	std::shared_ptr<PixelsFooterCache> pixelsFooterCache;
    pixels::proto::PostScript postScript;
    pixels::proto::Footer footer;
	bool closed;
};

#endif //PIXELS_PIXELSREADERIMPL_H
