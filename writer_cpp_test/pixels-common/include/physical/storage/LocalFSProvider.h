//
// Created by gengdy on 24-11-25.
//

#ifndef PIXELS_LOCALFSPROVIDER_H
#define PIXELS_LOCALFSPROVIDER_H

#include "physical/StorageProvider.h"
#include "physical/PhysicalWriter.h"
#include "physical/PhysicalWriterOption.h"

class LocalFSProvider : public StorageProvider {
public:
    std::shared_ptr<PhysicalWriter> createWriter(const std::string &path, std::shared_ptr<PhysicalWriterOption> option) override;
};
#endif //PIXELS_LOCALFSPROVIDER_H
