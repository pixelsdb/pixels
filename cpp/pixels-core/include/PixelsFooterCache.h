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
#ifndef PIXELS_PIXELSFOOTERCACHE_H
#define PIXELS_PIXELSFOOTERCACHE_H

#include <iostream>
#include <string>
#include "pixels-common/pixels.pb.h"
#include <unordered_map>

using namespace pixels::proto;
typedef std::unordered_map <std::string, std::shared_ptr<FileTail>> FileTailTable;
typedef std::unordered_map <std::string, std::shared_ptr<RowGroupFooter>> RGFooterTable;

class PixelsFooterCache
{
public:
    PixelsFooterCache();

    void putFileTail(const std::string &id, std::shared_ptr <FileTail> fileTail);

    bool containsFileTail(const std::string &id);

    std::shared_ptr <FileTail> getFileTail(const std::string &id);

    void putRGFooter(const std::string &id, std::shared_ptr <RowGroupFooter> footer);

    bool containsRGFooter(const std::string &id);

    std::shared_ptr <RowGroupFooter> getRGFooter(const std::string &id);

private:
    FileTailTable fileTailCacheMap;
    RGFooterTable rowGroupFooterCacheMap;

};
#endif //PIXELS_PIXELSFOOTERCACHE_H
