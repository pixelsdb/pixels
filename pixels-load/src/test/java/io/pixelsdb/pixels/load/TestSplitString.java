/*
 * Copyright 2021 PixelsDB.
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
package io.pixelsdb.pixels.load;

import org.junit.Test;

/**
 * Created at: 29/04/2021
 * Author: hank
 */
public class TestSplitString
{
    @Test
    public void test()
    {
        String s = "1|3689999|O|224560.83|1996-01-02|5-LOW|Clerk#000095055|0|nstructions sleep furiously among |";
        String reg = "\\\\|";
        String splits[] = s.split(reg);
        System.out.println(splits.length);
    }
}
