//
// Created by gengdy on 24-11-19.
//

#ifndef PIXELS_STATUS_H
#define PIXELS_STATUS_H

#include <string>

class Status {
public:
    Status();
    Status(const std::string &path, long long length, bool isDir, int replication);
    Status(const Status &other);
    long long getLength() const;
    bool isFile() const;
    bool isDirectory() const;
    short getReplication() const;
    std::string getPath() const;
    std::string getName() const;
    std::string toString() const;
    bool operator<(const Status &other) const;
    bool operator==(const Status &other) const;

private:
    std::string path;
    long long length;
    bool isDir;
    short replication;
};
#endif //PIXELS_STATUS_H
