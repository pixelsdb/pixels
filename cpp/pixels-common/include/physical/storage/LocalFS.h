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
#ifndef PIXELS_TEST_LOCALFS_H
#define PIXELS_TEST_LOCALFS_H

#include "physical/Storage.h"
#include "physical/natives/PixelsRandomAccessFile.h"
#include <string>
#include <vector>
#include <iostream>

/**
 * This implementation is used to access all kinds of POSIX file systems that are mounted
 * on a local directory. The file system does not need to be local physically. For example,
 * it could be a network file system mounted on a local point such as /mnt/nfs.
 *
 * @author liangyong
 * Created at: 02/03/2023
 */

class LocalFS : public Storage
{
public:
    LocalFS();

    ~LocalFS();

    Scheme getScheme() override;

    std::string ensureSchemePrefix(const std::string &path) const override;

    std::shared_ptr <PixelsRandomAccessFile> openRaf(const std::string &path);

    std::vector <std::string> listPaths(const std::string &path) override;

    std::ifstream open(const std::string &path) override;

    void close() override;

private:
    // TODO: read the configuration from pixels.properties for the following to values.
    static bool MmapEnabled;
    static bool EnableCache;
    static std::string SchemePrefix;
    // TODO: the remaining function is needed to be implemented.
};


#endif //PIXELS_TEST_LOCALFS_H
