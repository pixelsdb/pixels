//
// Created by liyu on 3/14/23.
//
#include "PixelsFooterCache.h"
#include "exception/InvalidArgumentException.h"

PixelsFooterCache::PixelsFooterCache() {
}

void PixelsFooterCache::putFileTail(const std::string& id, std::shared_ptr<FileTail> fileTail) {
    fileTailCacheMap[id] = fileTail;
}

std::shared_ptr<FileTail> PixelsFooterCache::getFileTail(const std::string& id) {
    if(fileTailCacheMap.find(id) != fileTailCacheMap.end()) {
        return fileTailCacheMap[id];
    } else {
        throw InvalidArgumentException("No such a FileTail id.");
    }
}

void PixelsFooterCache::putRGFooter(const std::string& id, std::shared_ptr<RowGroupFooter> footer) {
    rowGroupFooterCacheMap[id] = footer;
}

bool PixelsFooterCache::containsFileTail(const std::string &id) {
    return fileTailCacheMap.find(id) != fileTailCacheMap.end();
}

std::shared_ptr<RowGroupFooter> PixelsFooterCache::getRGFooter(const std::string& id) {
    if(rowGroupFooterCacheMap.find(id) != rowGroupFooterCacheMap.end()) {
        return rowGroupFooterCacheMap[id];
    } else {
        throw InvalidArgumentException("No such a RGFooter id.");
    }
}

bool PixelsFooterCache::containsRGFooter(const std::string &id) {
    return rowGroupFooterCacheMap.find(id) != rowGroupFooterCacheMap.end();
}


