//
// Created by liyu on 3/15/23.
//

#include "reader/PixelsReaderOption.h"

PixelsReaderOption::PixelsReaderOption() {
    // TODO: pixelsPredicate
    skipCorruptRecords = false;
    tolerantSchemaEvolution = true;
    enableEncodedColumnVector = true;
    enableFilterPushDown = false;
    queryId = -1L;
    rgStart = 0;
    rgLen = -1;  // -1 means reading to the end of the file
}

void PixelsReaderOption::setIncludeCols(const std::vector<std::string> & columnNames) {
    includedCols = columnNames;
}

std::vector<std::string> PixelsReaderOption::getIncludedCols() {
    return includedCols;
}

void PixelsReaderOption::setSkipCorruptRecords(bool s) {
    skipCorruptRecords = s;
}

bool PixelsReaderOption::isSkipCorruptRecords() {
    return skipCorruptRecords;
}

void PixelsReaderOption::setQueryId(long qId) {
    queryId = qId;
}

long PixelsReaderOption::getQueryId() {
    return queryId;
}

void PixelsReaderOption::setRGRange(int start, int len) {
    rgStart = start;
    rgLen = len;
}

int PixelsReaderOption::getRGStart() {
    return rgStart;
}

int PixelsReaderOption::getRGLen() {
    return rgLen;
}

void PixelsReaderOption::setTolerantSchemaEvolution(bool t) {
    tolerantSchemaEvolution = t;
}

bool PixelsReaderOption::isTolerantSchemaEvolution() {
    return tolerantSchemaEvolution;
}

void PixelsReaderOption::setEnableEncodedColumnVector(bool enabled) {
    enableEncodedColumnVector = enabled;
}

bool PixelsReaderOption::isEnableEncodedColumnVector() {
    return enableEncodedColumnVector;
}

void PixelsReaderOption::setEnabledFilterPushDown(bool enabledFilterPushDown) {
    this->enableFilterPushDown = enabledFilterPushDown;
}

bool PixelsReaderOption::isEnabledFilterPushDown() {
    return this->enableFilterPushDown;
}

void PixelsReaderOption::setFilter(duckdb::TableFilterSet * filter) {
    this->filter = filter;
}

duckdb::TableFilterSet * PixelsReaderOption::getFilter() {
    return this->filter;
}










