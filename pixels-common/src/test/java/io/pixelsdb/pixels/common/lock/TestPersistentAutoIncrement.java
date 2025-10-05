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
package io.pixelsdb.pixels.common.lock;

import io.pixelsdb.pixels.common.exception.EtcdException;
import io.pixelsdb.pixels.common.utils.Constants;
import org.junit.Test;

/**
 * @author hank
 * @create 2024-09-28
 */
public class TestPersistentAutoIncrement
{
    @Test
    public void test() throws EtcdException
    {
        PersistentAutoIncrement pai = new PersistentAutoIncrement(Constants.AI_TRANS_TS_KEY, true);
        for (int i = 0; i < 2048; ++i)
        {
            System.out.println(pai.getAndIncrement());
        }
    }
}
