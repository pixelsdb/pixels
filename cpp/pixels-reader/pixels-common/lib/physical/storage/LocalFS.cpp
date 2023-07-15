//
// Created by liyu on 2/28/23.
//

#include "physical/storage/LocalFS.h"
#include "physical/natives/DirectRandomAccessFile.h"
#include "physical/natives/DirectUringRandomAccessFile.h"

std::string LocalFS::SchemePrefix = "file://";

LocalFS::LocalFS() {

};

Storage::Scheme LocalFS::getScheme() {
    return file;
}

std::string LocalFS::ensureSchemePrefix(std::string path) {
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

void LocalFS::close() {
}

LocalFS::~LocalFS() = default;
