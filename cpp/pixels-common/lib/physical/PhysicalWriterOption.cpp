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
    return shared_from_this();
}

bool PhysicalWriterOption::isAddBlockPadding() const {
    return addBlockPadding;
}

std::shared_ptr<PhysicalWriterOption> PhysicalWriterOption::setAddBlockPadding(bool addBlockPadding) {
    this->addBlockPadding = addBlockPadding;
    return shared_from_this();
}

bool PhysicalWriterOption::isOverwrite() const {
    return overwrite;
}

std::shared_ptr<PhysicalWriterOption> PhysicalWriterOption::setOverwrite(bool overwrite) {
    this->overwrite = overwrite;
    return shared_from_this();
}
