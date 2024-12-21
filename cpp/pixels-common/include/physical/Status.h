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
