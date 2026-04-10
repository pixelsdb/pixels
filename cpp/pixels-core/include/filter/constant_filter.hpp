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
#include "table_filter.hpp"
#include <utility>
#include <variant>
#include <string>
#include <cstdint>

namespace pixels {

class ConstantFilter : public TableFilter {
public:
    static constexpr const TableFilterType TYPE = TableFilterType::CONSTANT_COMPARISON;
    
public:
    ConstantFilter(ComparisonOperator comparison_type, Scalar constant)
        : TableFilter(TYPE), 
          comparison_type(comparison_type), 
          constant(std::move(constant)) {}

    ComparisonOperator comparison_type;
    Scalar constant;
};

}