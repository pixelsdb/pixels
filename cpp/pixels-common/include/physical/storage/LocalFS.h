//
// Created by liyu on 2/28/23.
//

#ifndef PIXELS_TEST_LOCALFS_H
#define PIXELS_TEST_LOCALFS_H

#include "physical/Storage.h"
#include "physical/natives/PixelsRandomAccessFile.h"
/**
 * This implementation is used to access all kinds of POSIX file systems that are mounted
 * on a local directory. The file system does not need to be local physically. For example,
 * it could be a network file system mounted on a local point such as /mnt/nfs.
 *
 * @author liangyong
 * Created at: 02/03/2023
 */

class LocalFS: public Storage {
public:
    LocalFS();
    ~LocalFS();
    Scheme getScheme() override;
    std::string ensureSchemePrefix(std::string path) override;
	std::shared_ptr<PixelsRandomAccessFile> openRaf(const std::string& path);
    void close() override;
private:
    // TODO: read the configuration from pixels.properties for the following to values.
    static bool MmapEnabled;
    static bool EnableCache;
    static std::string SchemePrefix;
    // TODO: the remaining function is needed to be implemented.
};


#endif //PIXELS_TEST_LOCALFS_H
