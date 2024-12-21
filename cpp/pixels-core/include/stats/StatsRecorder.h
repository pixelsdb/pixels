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

/*
 * @author whz
 * @create 2024-11-19
 */

#ifndef PIXELS_STATSRECODER_H
#define PIXELS_STATSRECODER_H

#include "TypeDescription.h"
#include "pixels-common/pixels.pb.h"

class StatsRecorder {
protected:
    long numberOfValues;
    bool hasNull;

public:
    StatsRecorder();
    explicit StatsRecorder(const pixels::proto::ColumnStatistic& statistic);
    virtual ~StatsRecorder();

    void increment();
    void increment(long count);
    void setHasNull();

    virtual void updateBoolean(bool value, int repetitions);
    virtual void updateInteger(long value, int repetitions);
    virtual void updateInteger128(long high, long low, int repetitions);
    virtual void updateFloat(float value);
    virtual void updateDouble(double value);
    virtual void updateString(const std::string& value, int repetitions);
    virtual void updateBinary(const std::string& bytes, int repetitions);
    virtual void updateDate(int value);
    virtual void updateTime(int value);
    virtual void updateTimestamp(long value);
    virtual void updateVector();

    bool isStatsExists() const;
    void merge(const StatsRecorder& stats);
    void reset();

    long getNumberOfValues() const;
    bool hasNullValue() const;

    virtual pixels::proto::ColumnStatistic serialize() const;

    static std::unique_ptr<StatsRecorder> create(TypeDescription type);
    static std::unique_ptr<StatsRecorder> create(TypeDescription type, const pixels::proto::ColumnStatistic& statistic);
    static std::unique_ptr<StatsRecorder> create(TypeDescription::Category category, const pixels::proto::ColumnStatistic& statistic);
};
#endif // PIXELS_STATSRECODER_H
