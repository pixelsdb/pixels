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
package io.pixelsdb.pixels.common.utils;

import io.pixelsdb.pixels.common.exception.EtcdException;
import org.junit.Test;

import static io.pixelsdb.pixels.common.lock.EtcdAutoIncrement.GenerateId;

/**
 * @create 2021-08-28
 * @author hank
 */
public class TestAutoIncrement
{
    @Test
    public void testEtcdAutoIncrement() throws EtcdException
    {
        long id = GenerateId("test-id", true);
        System.out.println(id);
        id = GenerateId("test-id", true);
        System.out.println(id);
        id = GenerateId("test-id", false);
        System.out.println(id);
        id = GenerateId("test-id", false);
        System.out.println(id);
    }
}
