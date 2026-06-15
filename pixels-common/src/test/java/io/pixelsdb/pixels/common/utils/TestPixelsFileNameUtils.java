/*
 * Copyright 2026 PixelsDB.
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

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.OptionalLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests timestamp extraction from {@code .pxl} file names.
 */
public class TestPixelsFileNameUtils
{
    private static final String PXL_FILE_TIMESTAMP_ZONE_KEY = "pxl.file.timestamp.zone";
    private static final String DEFAULT_PXL_FILE_TIMESTAMP_ZONE = "UTC";

    @Test
    public void extractCreateTimeMillis_decodesEmbeddedTimestampUsingConfiguredDefaultZone()
    {
        ConfigFactory.Instance().addProperty(PXL_FILE_TIMESTAMP_ZONE_KEY,
                DEFAULT_PXL_FILE_TIMESTAMP_ZONE);
        String name = "host_20260514071200_0_3_ordered.pxl";
        long expected = LocalDateTime.of(2026, 5, 14, 7, 12, 0)
                .toInstant(ZoneOffset.UTC).toEpochMilli();

        OptionalLong actual = PixelsFileNameUtils.extractCreateTimeMillis(name);
        assertTrue("well-formed file name must decode", actual.isPresent());
        assertEquals(expected, actual.getAsLong());
    }

    @Test
    public void extractCreateTimeMillis_honorsConfiguredTimestampZone()
    {
        ConfigFactory.Instance().addProperty(PXL_FILE_TIMESTAMP_ZONE_KEY, "Asia/Shanghai");
        try
        {
            String name = "host_20260514071200_0_3_ordered.pxl";
            long expected = LocalDateTime.of(2026, 5, 14, 7, 12, 0)
                    .atZone(ZoneId.of("Asia/Shanghai")).toInstant().toEpochMilli();

            OptionalLong actual = PixelsFileNameUtils.extractCreateTimeMillis(name);
            assertTrue("well-formed file name must decode", actual.isPresent());
            assertEquals(expected, actual.getAsLong());
        }
        finally
        {
            ConfigFactory.Instance().addProperty(PXL_FILE_TIMESTAMP_ZONE_KEY,
                    DEFAULT_PXL_FILE_TIMESTAMP_ZONE);
        }
    }

    @Test
    public void extractCreateTimeMillis_roundTripsThroughDateUtilGetCurTimeWithConfiguredZone()
    {
        ConfigFactory.Instance().addProperty(PXL_FILE_TIMESTAMP_ZONE_KEY, "Asia/Shanghai");
        try
        {
            long before = System.currentTimeMillis();
            String name = "host_" + DateUtil.getCurTime() + "_3_ordered.pxl";
            long after = System.currentTimeMillis();

            OptionalLong decoded = PixelsFileNameUtils.extractCreateTimeMillis(name);
            assertTrue("DateUtil-generated filename must decode", decoded.isPresent());

            long beforeSec = (before / 1000L) * 1000L;
            long afterSec = ((after / 1000L) + 1L) * 1000L;
            assertTrue("decoded createTime " + decoded.getAsLong()
                            + " out of [" + beforeSec + ", " + afterSec + "]",
                    decoded.getAsLong() >= beforeSec && decoded.getAsLong() <= afterSec);
        }
        finally
        {
            ConfigFactory.Instance().addProperty(PXL_FILE_TIMESTAMP_ZONE_KEY,
                    DEFAULT_PXL_FILE_TIMESTAMP_ZONE);
        }
    }

    @Test
    public void extractCreateTimeMillis_handlesAbsolutePathPrefix()
    {
        ConfigFactory.Instance().addProperty(PXL_FILE_TIMESTAMP_ZONE_KEY,
                DEFAULT_PXL_FILE_TIMESTAMP_ZONE);
        String path = "/data/p/host_20200101000000_42_-1_single.pxl";
        long expected = LocalDateTime.of(2020, 1, 1, 0, 0, 0)
                .toInstant(ZoneOffset.UTC).toEpochMilli();

        OptionalLong actual = PixelsFileNameUtils.extractCreateTimeMillis(path);
        assertTrue(actual.isPresent());
        assertEquals(expected, actual.getAsLong());
    }

    @Test
    public void extractCreateTimeMillis_handlesHostnameWithUnderscores()
    {
        ConfigFactory.Instance().addProperty(PXL_FILE_TIMESTAMP_ZONE_KEY,
                DEFAULT_PXL_FILE_TIMESTAMP_ZONE);
        // Host names may contain underscores before the timestamp.
        String name = "retina_node_3_20260514071200_7_2_compact.pxl";
        long expected = LocalDateTime.of(2026, 5, 14, 7, 12, 0)
                .toInstant(ZoneOffset.UTC).toEpochMilli();

        OptionalLong actual = PixelsFileNameUtils.extractCreateTimeMillis(name);
        assertTrue(actual.isPresent());
        assertEquals(expected, actual.getAsLong());
    }

    @Test
    public void extractCreateTimeMillis_returnsEmptyOnUnrecognisedFormat()
    {
        assertFalse(PixelsFileNameUtils.extractCreateTimeMillis(null).isPresent());
        assertFalse(PixelsFileNameUtils.extractCreateTimeMillis("").isPresent());
        assertFalse(PixelsFileNameUtils.extractCreateTimeMillis("random.txt").isPresent());
        // Unknown file type label.
        assertFalse(PixelsFileNameUtils.extractCreateTimeMillis(
                "host_20260514071200_0_3_unknown.pxl").isPresent());
        // Timestamp must be exactly 14 digits.
        assertFalse(PixelsFileNameUtils.extractCreateTimeMillis(
                "host_2026051407120_0_3_ordered.pxl").isPresent());
    }

    @Test
    public void extractCreateTimeMillis_returnsEmptyOnStructurallyInvalidTimestamp()
    {
        // Structurally valid name with an invalid timestamp.
        OptionalLong actual = PixelsFileNameUtils.extractCreateTimeMillis(
                "host_20261314071200_0_3_ordered.pxl");
        assertFalse(actual.isPresent());
    }

    @Test
    public void extractCreateTimeMillis_roundTripsThroughDateUtilGetCurTime()
    {
        ConfigFactory.Instance().addProperty(PXL_FILE_TIMESTAMP_ZONE_KEY,
                DEFAULT_PXL_FILE_TIMESTAMP_ZONE);
        // DateUtil.getCurTime() should produce a decodable filename timestamp.
        long before = System.currentTimeMillis();
        String name = "host_" + DateUtil.getCurTime() + "_3_ordered.pxl";
        long after = System.currentTimeMillis();

        OptionalLong decoded = PixelsFileNameUtils.extractCreateTimeMillis(name);
        assertTrue("DateUtil-generated filename must decode", decoded.isPresent());

        // Decoded timestamp has second-level precision.
        long beforeSec = (before / 1000L) * 1000L;
        long afterSec = ((after / 1000L) + 1L) * 1000L;
        assertTrue("decoded createTime " + decoded.getAsLong()
                        + " out of [" + beforeSec + ", " + afterSec + "]",
                decoded.getAsLong() >= beforeSec && decoded.getAsLong() <= afterSec);
    }
}
