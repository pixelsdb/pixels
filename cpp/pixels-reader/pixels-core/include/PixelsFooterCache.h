//
// Created by liyu on 3/14/23.
//

#ifndef PIXELS_PIXELSFOOTERCACHE_H
#define PIXELS_PIXELSFOOTERCACHE_H

#include "tbb/concurrent_hash_map.h"
#include <iostream>
#include <string>
#include "pixels-common/pixels.pb.h"

using namespace tbb;
using namespace pixels::proto;
typedef concurrent_hash_map<std::string, std::shared_ptr<FileTail>> FileTailTable;
typedef concurrent_hash_map<std::string, std::shared_ptr<RowGroupFooter>> RGFooterTable;

class PixelsFooterCache {
public:
    PixelsFooterCache();
    void putFileTail(const std::string& id, std::shared_ptr<FileTail> fileTail);
    bool containsFileTail(const std::string& id);
	std::shared_ptr<FileTail> getFileTail(const std::string& id);
    void putRGFooter(const std::string& id, std::shared_ptr<RowGroupFooter> footer);
    bool containsRGFooter(const std::string& id);
	std::shared_ptr<RowGroupFooter> getRGFooter(const std::string& id);
private:
    FileTailTable fileTailCacheMap;
    RGFooterTable rowGroupFooterCacheMap;

};
#endif //PIXELS_PIXELSFOOTERCACHE_H
