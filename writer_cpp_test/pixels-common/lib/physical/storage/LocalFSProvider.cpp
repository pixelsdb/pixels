//
// Created by pixels on 24-11-25.
//

#include "physical/storage/LocalFSProvider.h"
#include "physical/storage/PhysicalLocalWriter.h"

std::shared_ptr <PhysicalWriter>
LocalFSProvider::createWriter(const std::string &path, std::shared_ptr <PhysicalWriterOption> option) {
    return std::static_pointer_cast<PhysicalWriter>(std::make_shared<PhysicalLocalWriter>(path, option->isOverwrite()));
}