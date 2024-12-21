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
 * @create 2023-03-14
 */
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


