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

import com.coreos.jetcd.data.KeyValue;
import org.junit.Test;

/**
 * Created at: 18-10-14
 * Author: hank
 */
public class TestEtcdUtil
{
    @Test
    public void test()
    {
        String key = "cache_version";
        EtcdUtil etcdUtil = EtcdUtil.Instance();
        long start = System.currentTimeMillis();
        //EtcdUtil.putEtcdKey(key, "hello world");
        KeyValue keyValue = etcdUtil.getKeyValue(key);
        long end = System.currentTimeMillis();
        System.out.println((end - start));
        if (keyValue != null)
            System.out.println("keyValue is：" + keyValue.getValue().toStringUtf8());
        else
            System.out.println("keyValue is：" + keyValue);
    }

}
