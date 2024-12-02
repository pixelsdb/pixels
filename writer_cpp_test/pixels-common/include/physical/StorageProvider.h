//
// Created by gengdy on 24-11-25.
//

#ifndef PIXELS_STORAGEPROVIDER_H
#define PIXELS_STORAGEPROVIDER_H

#include "physical/PhysicalWriter.h"
#include "physical/PhysicalWriterOption.h"
#include <memory>

class StorageProvider {
    virtual std::shared_ptr<PhysicalWriter> createWriter(const std::string &path, std::shared_ptr<PhysicalWriterOption> option) = 0;
};
#endif //PIXELS_STORAGEPROVIDER_H
