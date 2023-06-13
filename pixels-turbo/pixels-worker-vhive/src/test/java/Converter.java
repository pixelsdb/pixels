/*
 * Copyright 2023 PixelsDB.
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
import com.alibaba.fastjson.JSON;

import static org.junit.Assert.assertEquals;

public class Converter<T>
{
    final Class<T> typeParameterClass;

    public Converter(Class<T> typeParameterClass)
    {
        this.typeParameterClass = typeParameterClass;
    }

    public void executeTest(T input)
    {
        String json = JSON.toJSONString(input);
        T converted = JSON.parseObject(json, typeParameterClass);
        assertEquals(converted, input);
    }
}
