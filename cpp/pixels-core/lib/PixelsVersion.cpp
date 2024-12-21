/*
 * Copyright 2023 PixelsDB.
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
 * @author liyu
 * @create 2023-03-13
 */
#include "PixelsVersion.h"
#include "exception/InvalidArgumentException.h"

int PixelsVersion::getVersion() {
    return version;
}

PixelsVersion::PixelsVersion(int v) {
    version = v;
}

PixelsVersion::Version PixelsVersion::from(int v) {
    if (v == 1) {
        return V1;
    } else {
        throw InvalidArgumentException("Wrong pixels version. ");
    }
}

bool PixelsVersion::matchVersion(PixelsVersion::Version otherVersion) {
    return otherVersion == V1;
}

PixelsVersion::Version PixelsVersion::currentVersion() {
    return V1;
}




