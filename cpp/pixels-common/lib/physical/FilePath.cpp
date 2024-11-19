//
// Created by gengdy on 24-11-19.
//

#include <physical/FilePath.h>
#include <string>
#include <filesystem>

FilePath::FilePath()
    : realPath(""), valid(false), isDir(false) {}

FilePath::FilePath(const std::string &path)
    : realPath(""), valid(false), isDir(false) {
    if (path.empty()) {
        throw std::invalid_argument("path is null");
    }
    if (path.rfind("file:///", 0) == 0) {
        this->valid = true;
        this->realPath = path.substr(path.find("://") + 3);
    } else if (path.rfind("/", 0) == 0) {
        this->valid = true;
        this->realPath = path;
    }

    if (this->valid) {
        std::filesystem::path file(this->realPath);
        this->isDir = std::filesystem::is_directory(file);
    }
}

std::string FilePath::toString() const {
    if (!this->valid) {
        return "";
    }
    return this->realPath;
}

std::string FilePath::toStringWithPrefix(const Storage &storage) const {
    if (!this->valid) {
        return "";
    }
    return storage.ensureSchemePrefix(this->realPath);
}