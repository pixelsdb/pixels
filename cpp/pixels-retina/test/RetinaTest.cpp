/*
 * Copyright 2025 PixelsDB.
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

#include "gtest/gtest.h"
#include "Retina.h"

class RetinaBasicTest : public ::testing::Test {
protected:
    void SetUp() override {
        retina = new Retina(1000);
    }
    
    void TearDown() override {
        delete retina;
    }

    Retina* retina;
};

TEST_F(RetinaBasicTest, BasicDeleteAndVisibility) {
    uint64_t timestamp1 = 100;
    uint64_t timestamp2 = 200;

    retina->deleteRecord(5, timestamp1);
    retina->deleteRecord(10, timestamp1);
    retina->deleteRecord(15, timestamp2);
    retina->garbageCollect(timestamp1);

    uint64_t* bitmap1 = retina->getVisibilityBitmap(timestamp1);
    EXPECT_EQ(bitmap1[0], 0b0000010000100000);
    delete[] bitmap1;

    uint64_t* bitmap2 = retina->getVisibilityBitmap(timestamp2);
    EXPECT_EQ(bitmap2[0], 0b1000010000100000);
    delete[] bitmap2;
}
