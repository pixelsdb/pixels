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
 * @create 2023-03-15
 */
#include "reader/PixelsReaderOption.h"

PixelsReaderOption::PixelsReaderOption()
{
    // TODO: pixelsPredicate
    skipCorruptRecords = false;
    tolerantSchemaEvolution = true;
    enableEncodedColumnVector = true;
    enableFilterPushDown = false;
    queryId = -1L;
    batchSize = 0;
    rgStart = 0;
    rgLen = -1;  // -1 means reading to the end of the file
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

void PixelsReaderOption::setFilter(duckdb::TableFilterSet *filter)
{
    this->filter = filter;
}

duckdb::TableFilterSet *PixelsReaderOption::getFilter()
{
    return this->filter;
}

void PixelsReaderOption::setBatchSize(int batchSize)
{
    this->batchSize = batchSize;
}

int PixelsReaderOption::getBatchSize() const
{
    return batchSize;
}










