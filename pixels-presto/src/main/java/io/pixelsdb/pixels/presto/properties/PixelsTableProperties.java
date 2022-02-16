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
package io.pixelsdb.pixels.presto.properties;

import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;
import java.util.List;

import static com.facebook.presto.spi.session.PropertyMetadata.stringProperty;

/**
 * Class contains all table properties for the Pixels connector. Used when creating a table:
 * <p>
 * CREATE TABLE foo (a VARCHAR, b INT)
 * WITH (storage = 'hdfs');
 * </p>
 *
 * Created at: 14/02/2022
 * Author: hank
 */
public class PixelsTableProperties
{
    public static final String STORAGE = "storage";

    private final List<PropertyMetadata<?>> tableProperties;

    @Inject
    public PixelsTableProperties()
    {
        PropertyMetadata<String> s1 = stringProperty(
                STORAGE, "The storage scheme of the table.", "hdfs", false);

        tableProperties = ImmutableList.of(s1);
    }

    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }
}
