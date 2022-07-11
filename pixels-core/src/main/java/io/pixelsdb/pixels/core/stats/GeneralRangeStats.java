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
package io.pixelsdb.pixels.core.stats;

import io.pixelsdb.pixels.core.TypeDescription;

import java.math.BigDecimal;

/**
 * This is the general stats to be used by Trino and pixels-optimizer.
 *
 * @author hank
 * @date 11/07/2022
 */
public class GeneralRangeStats implements RangeStats<Double>
{
    private boolean hasMinimum = false;
    private boolean hasMaximum = false;
    private double minimum = Double.MIN_VALUE;
    private double maximum = Double.MAX_VALUE;

    public GeneralRangeStats(boolean hasMinimum, boolean hasMaximum, double minimum, double maximum)
    {
        this.hasMinimum = hasMinimum;
        this.hasMaximum = hasMaximum;
        this.minimum = minimum;
        this.maximum = maximum;
    }

    @Override
    public Double getMinimum()
    {
        return minimum;
    }

    @Override
    public Double getMaximum()
    {
        return maximum;
    }

    @Override
    public boolean hasMinimum()
    {
        return hasMinimum;
    }

    @Override
    public boolean hasMaximum()
    {
        return hasMaximum;
    }

    public static GeneralRangeStats fromStatsRecorder(TypeDescription type, IntegerStatsRecorder statsRecorder)
    {
        if (type.getCategory() == TypeDescription.Category.DECIMAL)
        {
            BigDecimal min = BigDecimal.valueOf(statsRecorder.getMinimum(), type.getScale());
            BigDecimal max = BigDecimal.valueOf(statsRecorder.getMaximum(), type.getScale());
            return new GeneralRangeStats(statsRecorder.hasMinimum(), statsRecorder.hasMaximum(),
                    min.doubleValue(), max.doubleValue());
        }
        else
        {
            return new GeneralRangeStats(statsRecorder.hasMinimum(), statsRecorder.hasMaximum(),
                    statsRecorder.getMinimum(), statsRecorder.getMaximum());
        }
    }

    public static GeneralRangeStats fromStatsRecorder(TypeDescription type, Integer128StatsRecorder statsRecorder)
    {
        if (type.getCategory() == TypeDescription.Category.DECIMAL)
        {
            BigDecimal min = new BigDecimal(statsRecorder.getMinimum().toBigInteger(), type.getScale());
            BigDecimal max = new BigDecimal(statsRecorder.getMaximum().toBigInteger(), type.getScale());
            return new GeneralRangeStats(statsRecorder.hasMinimum(), statsRecorder.hasMaximum(),
                    min.doubleValue(), max.doubleValue());
        }
        else
        {
            return new GeneralRangeStats(statsRecorder.hasMinimum(), statsRecorder.hasMaximum(),
                    statsRecorder.getMinimum().toBigInteger().doubleValue(),
                    statsRecorder.getMaximum().toBigInteger().doubleValue());
        }
    }

    public static GeneralRangeStats fromStatsRecorder(TypeDescription type, DoubleStatsRecorder statsRecorder)
    {
        return new GeneralRangeStats(statsRecorder.hasMinimum(), statsRecorder.hasMaximum(),
                statsRecorder.getMinimum(), statsRecorder.getMaximum());
    }

    public static GeneralRangeStats fromStatsRecorder(TypeDescription type, DateStatsRecorder statsRecorder)
    {
        return new GeneralRangeStats(statsRecorder.hasMinimum(), statsRecorder.hasMaximum(),
                statsRecorder.getMinimum(), statsRecorder.getMaximum());
    }

    public static GeneralRangeStats fromStatsRecorder(TypeDescription type, TimeStatsRecorder statsRecorder)
    {
        return new GeneralRangeStats(statsRecorder.hasMinimum(), statsRecorder.hasMaximum(),
                statsRecorder.getMinimum(), statsRecorder.getMaximum());
    }

    public static GeneralRangeStats fromStatsRecorder(TypeDescription type, TimestampStatsRecorder statsRecorder)
    {
        return new GeneralRangeStats(statsRecorder.hasMinimum(), statsRecorder.hasMaximum(),
                statsRecorder.getMinimum(), statsRecorder.getMaximum());
    }
}
