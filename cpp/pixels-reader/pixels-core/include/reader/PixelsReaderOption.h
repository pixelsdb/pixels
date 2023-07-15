//
// Created by liyu on 3/15/23.
//

#ifndef PIXELS_PIXELSREADEROPTION_H
#define PIXELS_PIXELSREADEROPTION_H

#include <iostream>
#include <string>
#include <vector>
class PixelsReaderOption {
public:
    PixelsReaderOption();
    void setIncludeCols(const std::vector<std::string> & columnNames);
    std::vector<std::string> getIncludedCols();
    void setSkipCorruptRecords(bool s);
    bool isSkipCorruptRecords();
    void setQueryId(long qId);
    long getQueryId();
    void setRGRange(int start, int len);
    int getRGStart();
    int getRGLen();
    void setTolerantSchemaEvolution(bool t);
    bool isTolerantSchemaEvolution();
    void setEnableEncodedColumnVector(bool enabled);
    bool isEnableEncodedColumnVector();
private:
    std::vector<std::string> includedCols;
    // TODO: pixelsPredicate
    bool skipCorruptRecords;
    bool tolerantSchemaEvolution;     // this may lead to column missing due to schema evolution
    bool enableEncodedColumnVector;   // whether read encoded column vectors directly when possible
    long queryId;
    int rgStart;
    int rgLen;
};
#endif //PIXELS_PIXELSREADEROPTION_H
