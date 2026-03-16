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

//仿照duckdb设计
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