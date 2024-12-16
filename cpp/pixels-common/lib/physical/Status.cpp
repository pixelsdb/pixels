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

//
// Created by gengdy on 24-11-19.
//

#include <physical/Status.h>
#include <string>
#include <sstream>

Status::Status() : path(""), length(0), isDir(false), replication(0) {}

Status::Status(const std::string &path, uint64_t length, bool isDir, int replication)
    : path(path), length(length), isDir(isDir), replication(static_cast<short>(replication)) {}

Status::Status(const Status &other)
    : path(other.path), length(other.length), isDir(other.isDir), replication(other.replication) {}

uint64_t Status::getLength() const {
    return this->length;
}

bool Status::isFile() const {
    return !this->isDir;
}

bool Status::isDirectory() const {
    return this->isDir;
}

short Status::getReplication() const {
    return this->replication;
}

std::string Status::getPath() const {
    return this->path;
}

std::string Status::getName() const {
    size_t slash = path.find_last_of('/');
    return (slash == std::string::npos) ? path : path.substr(slash + 1);
}
bool Status::operator<(const Status &other) const
 {
    return this->path < other.path;
}

bool Status::operator==(const Status &other) const {
    return this->path == other.path;
}

std::string Status::toString() const {
    std::ostringstream sb;
    sb << "Status{path=" << path << "; isDirectory=" << (isDir ? "true" : "false");
    if (!isDirectory()) {
        sb << "; length=" << length;
    }
    sb << "}";
    return sb.str();
}