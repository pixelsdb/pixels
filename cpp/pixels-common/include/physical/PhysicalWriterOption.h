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
 * @create 2024-11-25
 */
#ifndef PIXELS_PHYSICALWRITEROPTION_H
#define PIXELS_PHYSICALWRITEROPTION_H

#include <memory>
#include <cstdint>

class PhysicalWriterOption : public std::enable_shared_from_this<PhysicalWriterOption> {
public:
    PhysicalWriterOption(std::int64_t blockSize, bool addBlockPadding, bool overwrite);
    std::int64_t getBlockSize() const;
    std::shared_ptr<PhysicalWriterOption> setBlockSize(std::int64_t blockSize);
    bool isAddBlockPadding() const;
    std::shared_ptr<PhysicalWriterOption> setAddBlockPadding(bool addBlockPadding);
    bool isOverwrite() const;
    std::shared_ptr<PhysicalWriterOption> setOverwrite(bool overwrite);
private:
    std::int64_t blockSize;
    bool addBlockPadding;
    bool overwrite;
};
#endif //PIXELS_PHYSICALWRITEROPTION_H
