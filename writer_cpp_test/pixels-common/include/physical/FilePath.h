//
// Created by gengdy on 24-11-19.
//

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
