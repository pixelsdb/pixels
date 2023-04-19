/*
 * Copyright 2022 PixelsDB.
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
package io.pixelsdb.pixels.core.utils;

import org.junit.Test;

/**
 * @create 2022-04-09
 * @author hank
 */
public class TestBitmap
{
    @Test
    public void testSetClear()
    {
        Bitmap bitmap = new Bitmap(100, false);
        assert bitmap.cardinality() == 0;
        assert bitmap.length() == 0;
        bitmap.setAll();
        assert bitmap.cardinality() == 100;
        assert bitmap.length() == 100;
        bitmap.clearAll();
        assert bitmap.cardinality() == 0;
        assert bitmap.length() == 0;
    }

    @Test
    public void testCardinality()
    {
        Bitmap bitmap = new Bitmap(200, false);
        bitmap.set(72, 100);
        assert bitmap.cardinality(66, 120) == 28;
        assert bitmap.cardinality(80, 90) == 10;
        bitmap.set(56, 168);
        assert bitmap.cardinality(32, 180) == 112;
        assert bitmap.cardinality(32, 160) == 104;
        assert bitmap.cardinality(68, 200) == 100;
        assert bitmap.cardinality() == 112;
    }
}
