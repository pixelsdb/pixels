/*
 * Copyright 2018 PixelsDB.
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
package io.pixelsdb.pixels.common.utils;

import org.junit.Test;

/**
 * @author guodong
 */
public class TestStringUtil
{
    @Test
    public void testReplaceAll()
    {
        String s = "abcdTrueidsdFalseddtruetureddssd";
        s = StringUtil.replaceAll(s, "True", "1");
        s = StringUtil.replaceAll(s, "False", "0");
        s = StringUtil.replaceAll(s, "true", "1");
        s = StringUtil.replaceAll(s, "false", "0");
        System.out.println(s);
    }
}
