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
#ifndef PIXELS_PHYSICALWRITERUTIL_H
#define PIXELS_PHYSICALWRITERUTIL_H

#include "physical/PhysicalWriter.h"
#include "physical/PhysicalWriterOption.h"
#include "physical/storage/LocalFSProvider.h"

class PhysicalWriterUtil
{
public:
    static std::shared_ptr <PhysicalWriter> newPhysicalWriter(std::string path, int blockSize,
                                                              bool blockPadding, bool overwrite)
    {
        std::shared_ptr <PhysicalWriterOption> option = std::make_shared<PhysicalWriterOption>(blockSize, blockPadding,
                                                                                               overwrite);
        LocalFSProvider provider;
        return provider.createWriter(path, option);
    }
};
#endif //PIXELS_PHYSICALWRITERUTIL_H
