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

#pragma once

#include <map>
#include <string>
#include "PixelsTypes.h"
#include <memory>
#include <vector>
namespace pixels {

//! TableFilter represents a filter pushed down into the table scan.
class TableFilter {
public:
	TableFilter() : filter_type(TableFilterType::DEFAULT) {}
	explicit TableFilter(TableFilterType filter_type_p) : filter_type(filter_type_p) {
	}
	virtual ~TableFilter() {
	}

	TableFilterType filter_type;

public:
	virtual bool Equals(const TableFilter &other) const {
		return filter_type == other.filter_type;
	}
};

//! TableFilterSet represents a set of filters on different columns. It is used for filter pushdown in table scan.
class TableFilterSet {
public:
	std::map<idx_t, std::unique_ptr<TableFilter>> filters;
public:
	bool Equals(TableFilterSet &other) {
		if (filters.size() != other.filters.size()) {
			return false;
		}
		for (auto &entry : filters) {
			auto other_entry = other.filters.find(entry.first);
			if (other_entry == other.filters.end()) {
				return false;
			}
			if (!entry.second->Equals(*other_entry->second)) {
				return false;
			}
		}
		return true;
	}
	static bool Equals(TableFilterSet *left, TableFilterSet *right) {
		if (left == right) {
			return true;
		}
		if (!left || !right) {
			return false;
		}
		return left->Equals(*right);
	}
};

} // namespace pixels