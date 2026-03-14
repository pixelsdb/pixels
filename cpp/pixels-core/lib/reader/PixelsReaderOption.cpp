#include "reader/PixelsReaderOption.h"

PixelsReaderOption::PixelsReaderOption()
{
    skipCorruptRecords = false;
    tolerantSchemaEvolution = true;
    enableEncodedColumnVector = true;
    enableFilterPushDown = false; 
    queryId = -1L;
    batchSize = 0;
    rgStart = 0;
    rgLen = -1;  // -1 means reading to the end of the file
    ringIndex = 0; // 显式初始化
}

void PixelsReaderOption::setIncludeCols(const std::vector <std::string> &columnNames)
{
    includedCols = columnNames;
}

std::vector <std::string> PixelsReaderOption::getIncludedCols()
{
    return includedCols;
}

void PixelsReaderOption::setSkipCorruptRecords(bool s)
{
    skipCorruptRecords = s;
}

bool PixelsReaderOption::isSkipCorruptRecords()
{
    return skipCorruptRecords;
}

void PixelsReaderOption::setQueryId(long qId)
{
    queryId = qId;
}

long PixelsReaderOption::getQueryId()
{
    return queryId;
}

int PixelsReaderOption::getRingIndex() const {
    return ringIndex;
}

void PixelsReaderOption::setRingIndex(int i) {
    ringIndex = i;
}

void PixelsReaderOption::setRGRange(int start, int len)
{
    rgStart = start;
    rgLen = len;
}

int PixelsReaderOption::getRGStart()
{
    return rgStart;
}

int PixelsReaderOption::getRGLen()
{
    return rgLen;
}

void PixelsReaderOption::setTolerantSchemaEvolution(bool t)
{
    tolerantSchemaEvolution = t;
}

bool PixelsReaderOption::isTolerantSchemaEvolution()
{
    return tolerantSchemaEvolution;
}

void PixelsReaderOption::setEnableEncodedColumnVector(bool enabled)
{
    enableEncodedColumnVector = enabled;
}

bool PixelsReaderOption::isEnableEncodedColumnVector()
{
    return enableEncodedColumnVector;
}

void PixelsReaderOption::setEnabledFilterPushDown(bool enabledFilterPushDown)
{
    this->enableFilterPushDown = enabledFilterPushDown;
}

bool PixelsReaderOption::isEnabledFilterPushDown()
{
    return this->enableFilterPushDown;
}


void PixelsReaderOption::setFilter(pixels::TableFilterSet f) {
    //std::cout<<"at set filter"<<f.filters.size()<<std::endl;
    filter = std::move(f);
}

pixels::TableFilterSet PixelsReaderOption::extractFilter() {
    return std::move(filter);
}

int PixelsReaderOption::getfiltersize(){
    return filter.filters.size();
}

void PixelsReaderOption::setBatchSize(int batchSize)
{
    this->batchSize = batchSize;
}

int PixelsReaderOption::getBatchSize() const
{
    return batchSize;
}