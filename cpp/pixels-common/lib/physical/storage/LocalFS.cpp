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
 * @create 2023-02-28
 */
#include "physical/storage/LocalFS.h"
#include "physical/natives/DirectRandomAccessFile.h"
#include "physical/natives/DirectUringRandomAccessFile.h"
#include "physical/FilePath.h"
#include <filesystem>
namespace fs = std::filesystem;

std::string LocalFS::SchemePrefix = "file://";

LocalFS::LocalFS() {

};

Storage::Scheme LocalFS::getScheme() {
    return file;
}

std::string LocalFS::ensureSchemePrefix(const std::string &path) const {
    if(path.rfind(SchemePrefix, 0) != std::string::npos) {
        return path;
    }
    if(path.find("://") != std::string::npos) {
        throw std::invalid_argument("Path '" + path +
                             "' already has a different scheme prefix than '" + SchemePrefix + "'.");
    }
    return SchemePrefix + path;
}

std::shared_ptr<PixelsRandomAccessFile> LocalFS::openRaf(const std::string& path) {
    if(true) {
        // TODO: change this class to mmap class in the future.
        return std::make_shared<DirectUringRandomAccessFile>(path);
    } else {
        return std::make_shared<DirectRandomAccessFile>(path);
    }
}

std::vector<std::string> LocalFS::listPaths(const std::string &path) {
    std::vector<std::string> paths;
    FilePath p(path);
    if (!p.valid) {
        throw std::runtime_error("Path " + path + " is not a valid local fs path.");
    }

    fs::path file(p.realPath);
    std::vector<fs::directory_entry> files;
    if (fs::is_directory(file)) {
        for (const auto &entry : fs::directory_iterator(file)) {
            files.push_back(entry);
        }
    } else {
        if (fs::exists(file)) {
            files.push_back(fs::directory_entry(file));
        }
    }
    if (files.empty()) {
        throw std::runtime_error("Failed to list files in path: " + p.realPath + ".");
    } else {
        for (const auto &eachFile : files) {
            paths.push_back(ensureSchemePrefix(eachFile.path().string()));
        }
    }
    return paths;
}

std::ifstream LocalFS::open(const std::string &path) {
    FilePath p(path);
    if (!p.valid) {
        throw std::runtime_error("Path '" + path + "' is not a valid local fs path.");
    }
    fs::path file(p.realPath);
    if (fs::is_directory(file)) {
        throw std::runtime_error("Path '" + p.realPath + "' is a directory, it must be a file.");
    }
    if (!fs::exists(file)) {
        throw std::runtime_error("File '" + p.realPath + "' doesn't exists.");
    }
    return std::ifstream(file);
}

void LocalFS::close() {
}

LocalFS::~LocalFS() = default;
