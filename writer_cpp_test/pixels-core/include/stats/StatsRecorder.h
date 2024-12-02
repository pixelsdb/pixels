//
// Created by whz on 11/19/24.
//

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
