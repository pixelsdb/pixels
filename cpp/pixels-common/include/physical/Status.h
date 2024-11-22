//
// Created by gengdy on 24-11-19.
//

#ifndef PIXELS_STATUS_H
#define PIXELS_STATUS_H

#include <string>
#include <cstdint>

class Status {
public:
    Status();
    Status(const std::string &path, uint64_t length, bool isDir, int replication);
    Status(const Status &other);
    uint64_t getLength() const;
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
    uint64_t length;
    bool isDir;
    short replication;
};
#endif //PIXELS_STATUS_H
