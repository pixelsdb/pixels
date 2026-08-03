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
package io.pixelsdb.pixels.core.vector;

import io.pixelsdb.pixels.core.TypeDescription;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

/**
 * Created at: 06/03/2022
 * Author: hank
 */
public class TestDecimalColumnVector
{
    @Test
    public void testAddCanonicalBytes()
    {
        String[] values = {"5755.94", "-283.84", "0", "-2"};
        TypeDescription type = TypeDescription.createDecimal(15, 2);
        DecimalColumnVector fromBytes = new DecimalColumnVector(values.length, 15, 2);
        DecimalColumnVector fromStrings = new DecimalColumnVector(values.length, 15, 2);
        for (String value : values)
        {
            fromBytes.add(type.convertSqlStringToByte(value));
            fromStrings.add(value);
        }
        assertArrayEquals(fromStrings.vector, fromBytes.vector);
    }

    @Test
    public void testAddStringVal()
    {
        DecimalColumnVector columnVector = new DecimalColumnVector(10, 15, 2);
        columnVector.add("5755.94");
        columnVector.add("4032.68");
        columnVector.add("4192.40");
        columnVector.add("4641.08");
        columnVector.add("-283.84");
        columnVector.add("1365.79");
        columnVector.add("0");
        columnVector.add("0.000");
        columnVector.add("1.2345");
        columnVector.add("-2");
        for (int i = 0; i < 10; ++i)
        {
            System.out.println(columnVector.vector[i]);
        }
    }

    @Test
    public void testAddDoubleVal()
    {
        DecimalColumnVector columnVector = new DecimalColumnVector(10, 15, 2);
        columnVector.add(5755.94);
        columnVector.add(4032.68);
        columnVector.add(4192.40);
        columnVector.add(4641.08);
        columnVector.add(-283.84);
        columnVector.add(1365.79);
        columnVector.add(0.0);
        columnVector.add(0.000);
        columnVector.add(1.2345);
        columnVector.add(-2.0);
        for (int i = 0; i < 10; ++i)
        {
            System.out.println(columnVector.vector[i]);
        }
    }
}
