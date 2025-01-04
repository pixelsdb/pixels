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

#include "visibility/Visibility.h"
#include <iostream>
#include <bitset>

void testVisibilitySingleThread()
{
    Visibility visibility;
    visibility.createNewEpoch(0);
    visibility.deleteRecord(1, 90);
    visibility.deleteRecord(100, 200);
    visibility.deleteRecord(200, 400);
    std::uint64_t bitmap[4];
    visibility.getVisibilityBitmap(0, bitmap);
    for (int i = 0; i < 4; i++)
    {
        std::cout << std::bitset<64>(bitmap[i]) << std::endl;
    }
    std::cout << "----------------" << std::endl;
    visibility.createNewEpoch(1000);
    visibility.deleteRecord(2, 90);
    visibility.getVisibilityBitmap(0, bitmap);
    for (int i = 0; i < 4; i++)
    {
        std::cout << std::bitset<64>(bitmap[i]) << std::endl;
    }
}
void testVisibilityMultiThread() {
}
int main() {
    std::cout << "Running single-threaded test..." << std::endl;
    testVisibilitySingleThread();
    return 0;
}