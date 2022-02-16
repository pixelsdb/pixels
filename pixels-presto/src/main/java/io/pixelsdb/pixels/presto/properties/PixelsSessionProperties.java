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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;
import java.util.List;

import static com.facebook.presto.spi.session.PropertyMetadata.booleanProperty;

/**
 * Created at: 15/02/2022
 * Author: hank
 */
public class PixelsSessionProperties
{
    private static final String ORDERED_PATH_ENABLED = "ordered_path_enabled";
    private static final String COMPACT_PATH_ENABLED = "compact_path_enabled";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public PixelsSessionProperties()
    {
        PropertyMetadata<Boolean> s1 = booleanProperty(
                ORDERED_PATH_ENABLED,
                "Set to true to enable the ordered path for queries.", true, false);

        PropertyMetadata<Boolean> s2 = booleanProperty(
                COMPACT_PATH_ENABLED,
                "Set to true to enable the compact path for queries.", true, false);

        sessionProperties = ImmutableList.of(s1, s2);
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static boolean getOrderedPathEnabled(ConnectorSession session)
    {
        return session.getProperty(ORDERED_PATH_ENABLED, Boolean.class);
    }

    public static boolean getCompactPathEnabled(ConnectorSession session)
    {
        return session.getProperty(COMPACT_PATH_ENABLED, Boolean.class);
    }
}
