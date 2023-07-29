/*
 * Copyright 2021-2023 PixelsDB.
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
package io.pixelsdb.pixels.common.task;

import io.pixelsdb.pixels.common.metadata.domain.Base;
import org.junit.Test;

/**
 * @author hank
 * @create 2023-07-26
 */
public class TestQueue
{
    @Test
    public void testTaskConstruction()
    {
        Task<Base> task = new Task<>("123", "{\"id\":456}", Base.class, 60000);
        System.out.println(task.getId());
        System.out.println(task.getPayloadJson());
        System.out.println(task.getPayload().getId());
    }
}
