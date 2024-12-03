//
// Created by whz on 11/19/24.
//

#include "stats/StatsRecorder.h"
#include <stdexcept>


StatsRecorder::StatsRecorder() : numberOfValues(0), hasNull(false) {}


StatsRecorder::StatsRecorder(const pixels::proto::ColumnStatistic& statistic)
        : numberOfValues(statistic.has_numberofvalues() ? statistic.numberofvalues() : 0),
          hasNull(statistic.has_hasnull() ? statistic.hasnull() : true) {}


StatsRecorder::~StatsRecorder() = default;


void StatsRecorder::increment() { numberOfValues++; }

void StatsRecorder::increment(long count) { numberOfValues += count; }


void StatsRecorder::setHasNull() { hasNull = true; }


void StatsRecorder::updateBoolean(bool, int) {
    throw std::logic_error("Can't update boolean");
}

void StatsRecorder::updateInteger(long, int) {
    throw std::logic_error("Can't update integer");
}

void StatsRecorder::updateInteger128(long, long, int) {
    throw std::logic_error("Can't update integer128");
}

void StatsRecorder::updateFloat(float) {
    throw std::logic_error("Can't update float");
}

void StatsRecorder::updateDouble(double) {
    throw std::logic_error("Can't update double");
}

void StatsRecorder::updateString(const std::string&, int) {
    throw std::logic_error("Can't update string");
}

void StatsRecorder::updateBinary(const std::string&, int) {
    throw std::logic_error("Can't update binary");
}

void StatsRecorder::updateDate(int) {
    throw std::logic_error("Can't update date");
}

void StatsRecorder::updateTime(int) {
    throw std::logic_error("Can't update time");
}

void StatsRecorder::updateTimestamp(long) {
    throw std::logic_error("Can't update timestamp");
}

void StatsRecorder::updateVector() {
    throw std::logic_error("Can't update vector");
}

bool StatsRecorder::isStatsExists() const {
    return (numberOfValues > 0 || hasNull);
}


void StatsRecorder::merge(const StatsRecorder& stats) {
    numberOfValues += stats.numberOfValues;
    hasNull |= stats.hasNull;
}


void StatsRecorder::reset() {
    numberOfValues = 0;
    hasNull = false;
}


long StatsRecorder::getNumberOfValues() const { return numberOfValues; }

bool StatsRecorder::hasNullValue() const { return hasNull; }


pixels::proto::ColumnStatistic StatsRecorder::serialize() const {
    pixels::proto::ColumnStatistic statistic;
    statistic.set_numberofvalues(numberOfValues);
    statistic.set_hasnull(hasNull);
    return statistic;
}


std::unique_ptr<StatsRecorder> StatsRecorder::create(TypeDescription type) {
    switch (type.getCategory()) {

        case TypeDescription::BOOLEAN:
            // return std::make_unique<BooleanStatsRecorder>();
            break;

        default:
            return std::make_unique<StatsRecorder>();
    }
}


std::unique_ptr<StatsRecorder> StatsRecorder::create(TypeDescription type, const pixels::proto::ColumnStatistic& statistic) {
    switch (type.getCategory()) {

        default:
            return std::make_unique<StatsRecorder>(statistic);
    }
}


std::unique_ptr<StatsRecorder> StatsRecorder::create(TypeDescription::Category category, const pixels::proto::ColumnStatistic& statistic) {
    switch (category) {

        default:
            return std::make_unique<StatsRecorder>(statistic);
    }
}
