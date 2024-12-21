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
