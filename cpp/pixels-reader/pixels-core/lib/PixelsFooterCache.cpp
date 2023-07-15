//
// Created by liyu on 3/14/23.
//
#include "PixelsFooterCache.h"
#include "exception/InvalidArgumentException.h"

PixelsFooterCache::PixelsFooterCache() {
    fileTailCacheMap = FileTailTable(200);
    rowGroupFooterCacheMap = RGFooterTable(200);
}

void PixelsFooterCache::putFileTail(const std::string& id, std::shared_ptr<FileTail> fileTail) {
    FileTailTable::accessor accessor;
    if(fileTailCacheMap.insert(accessor, id)) {
        accessor->second = fileTail;
    }
}

std::shared_ptr<FileTail> PixelsFooterCache::getFileTail(const std::string& id) {
    FileTailTable::accessor accessor;
    if(fileTailCacheMap.find(accessor, id)) {
        return accessor->second;
    } else {
        throw InvalidArgumentException("No such a FileTail id.");
    }
}

void PixelsFooterCache::putRGFooter(const std::string& id, std::shared_ptr<RowGroupFooter> footer) {
    RGFooterTable::accessor accessor;
    if(rowGroupFooterCacheMap.insert(accessor, id)) {
        accessor->second = footer;
    }
}

bool PixelsFooterCache::containsFileTail(const std::string &id) {
    return fileTailCacheMap.count(id) > 0;
}

std::shared_ptr<RowGroupFooter> PixelsFooterCache::getRGFooter(const std::string& id) {
    RGFooterTable::accessor accessor;
    if(rowGroupFooterCacheMap.find(accessor, id)) {
        return accessor->second;
    } else {
        throw InvalidArgumentException("No such a RGFooter id.");
    }
}

bool PixelsFooterCache::containsRGFooter(const std::string &id) {
    return rowGroupFooterCacheMap.count(id) > 0;
}


