#pragma once

#include "table_filter.hpp"


namespace pixels {

class ConjunctionFilter : public TableFilter {
public:
	explicit ConjunctionFilter(TableFilterType filter_type_p) : TableFilter(filter_type_p) {
	}

	~ConjunctionFilter() override {
	}

	//! The filters of this conjunction
	std::vector<std::unique_ptr<TableFilter>> child_filters;

public:
	bool Equals(const TableFilter &other) const override {
		return TableFilter::Equals(other);
	}
};

class ConjunctionOrFilter : public ConjunctionFilter {
public:
    static constexpr const TableFilterType TYPE = TableFilterType::CONJUNCTION_OR;

    ConjunctionOrFilter()
        : ConjunctionFilter(TYPE) {}
};

class ConjunctionAndFilter : public ConjunctionFilter {
public:
    static constexpr const TableFilterType TYPE = TableFilterType::CONJUNCTION_AND;

    ConjunctionAndFilter()
        : ConjunctionFilter(TYPE) {}
};

}