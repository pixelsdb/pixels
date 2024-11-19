//
// Created by gengdy on 24-11-19.
//

#include <physical/Status.h>
#include <string>
#include <sstream>

Status::Status() : path(""), length(0), isDir(false), replication(0) {}

Status::Status(const std::string &path, long long length, bool isDir, int replication)
    : path(path), length(length), isDir(isDir), replication(static_cast<short>(replication)) {}

Status::Status(const Status &other)
    : path(other.path), length(other.length), isDir(other.isDir), replication(other.replication) {}

long long Status::getLength() const {
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