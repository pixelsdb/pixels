//
// Created by gengdy on 24-11-25.
//

#ifndef PIXELS_PHYSICALWRITERUTIL_H
#define PIXELS_PHYSICALWRITERUTIL_H

#include "physical/PhysicalWriter.h"
#include "physical/PhysicalWriterOption.h"
#include "physical/storage/LocalFSProvider.h"

class PhysicalWriterUtil {
public:
    static std::shared_ptr<PhysicalWriter> newPhysicalWriter(std::string path, int blockSize,
                                                             bool blockPadding, bool overwrite) {
        std::shared_ptr<PhysicalWriterOption> option = std::make_shared<PhysicalWriterOption>(blockSize, blockPadding, overwrite);
        LocalFSProvider provider;
        return provider.createWriter(path, option);
    }
};
#endif //PIXELS_PHYSICALWRITERUTIL_H
