/*
 * Copyright 2024 PixelsDB.
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
 * @author gengdy
 * @create 2024-11-19
 */
#ifndef PIXELS_FILEPATH_H
#define PIXELS_FILEPATH_H

#include <string>
#include <physical/Storage.h>

class FilePath {
public:
    std::string realPath;
    bool valid;
    bool isDir;

    FilePath();
    FilePath(const std::string &path);
    std::string toString() const;
    std::string toStringWithPrefix(const Storage &storage) const;
};
#endif //PIXELS_FILEPATH_H
