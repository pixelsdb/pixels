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
#include <physical/FilePath.h>
#include <string>
#include <filesystem>

FilePath::FilePath()
        : realPath(""), valid(false), isDir(false)
{}

FilePath::FilePath(const std::string &path)
        : realPath(""), valid(false), isDir(false)
{
    if (path.empty())
    {
        throw std::invalid_argument("path is null");
    }
    if (path.rfind("file:///", 0) == 0)
    {
        this->valid = true;
        this->realPath = path.substr(path.find("://") + 3);
    }
    else if (path.rfind("/", 0) == 0)
    {
        this->valid = true;
        this->realPath = path;
    }

    if (this->valid)
    {
        std::filesystem::path file(this->realPath);
        this->isDir = std::filesystem::is_directory(file);
    }
}

std::string FilePath::toString() const
{
    if (!this->valid)
    {
        return "";
    }
    return this->realPath;
}

std::string FilePath::toStringWithPrefix(const Storage &storage) const
{
    if (!this->valid)
    {
        return "";
    }
    return storage.ensureSchemePrefix(this->realPath);
}