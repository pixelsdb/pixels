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
package io.pixelsdb.pixels.daemon.metadata.dao;

import io.pixelsdb.pixels.daemon.metadata.dao.impl.EtcdCommon;
import org.junit.Test;

import static io.pixelsdb.pixels.daemon.metadata.dao.impl.EtcdCommon.schemaIdKey;
import static io.pixelsdb.pixels.daemon.metadata.dao.impl.EtcdCommon.schemaIdLockPath;

/**
 * Created at: 8/28/21
 * Author: hank
 */
public class TestEtcdDaos
{
    @Test
    public void testEtcdDao()
    {
        long id = EtcdCommon.generateId(schemaIdKey, schemaIdLockPath);
        System.out.println(id);
        id = EtcdCommon.generateId(schemaIdKey, schemaIdLockPath);
        System.out.println(id);
        id = EtcdCommon.generateId(schemaIdKey, schemaIdLockPath);
        System.out.println(id);
        id = EtcdCommon.generateId(schemaIdKey, schemaIdLockPath);
        System.out.println(id);
    }
}
