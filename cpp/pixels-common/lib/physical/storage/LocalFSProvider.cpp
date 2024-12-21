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
#include "physical/storage/LocalFSProvider.h"
#include "physical/storage/PhysicalLocalWriter.h"

std::shared_ptr <PhysicalWriter>
LocalFSProvider::createWriter(const std::string &path, std::shared_ptr <PhysicalWriterOption> option)
{
    return std::static_pointer_cast<PhysicalWriter>(std::make_shared<PhysicalLocalWriter>(path, option->isOverwrite()));
}