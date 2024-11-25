//
// Created by gengdy on 24-11-25.
//

#include "physical/PhysicalWriterOption.h"

PhysicalWriterOption::PhysicalWriterOption(std::int64_t blockSize, bool addBlockPadding, bool overwrite)
    : blockSize(blockSize), addBlockPadding(addBlockPadding), overwrite(overwrite) {}

std::int64_t PhysicalWriterOption::getBlockSize() const {
    return blockSize;
}

std::shared_ptr<PhysicalWriterOption> PhysicalWriterOption::setBlockSize(std::int64_t blockSize) {
    this->blockSize = blockSize;
    return std::shared_ptr<PhysicalWriterOption>(this);
}

bool PhysicalWriterOption::isAddBlockPadding() const {
    return addBlockPadding;
}

std::shared_ptr<PhysicalWriterOption> PhysicalWriterOption::setAddBlockPadding(bool addBlockPadding) {
    this->addBlockPadding = addBlockPadding;
    return std::shared_ptr<PhysicalWriterOption>(this);
}

bool PhysicalWriterOption::isOverwrite() const {
    return overwrite;
}

std::shared_ptr<PhysicalWriterOption> PhysicalWriterOption::setOverwrite(bool overwrite) {
    this->overwrite = overwrite;
    return std::shared_ptr<PhysicalWriterOption>(this);
}
