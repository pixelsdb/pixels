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
    // 使用 std::move 处理可能存在的字符串拷贝
    ConstantFilter(ComparisonOperator comparison_type, Scalar constant)
        : TableFilter(TYPE), 
          comparison_type(comparison_type), 
          constant(std::move(constant)) {}

    //! 比较操作符 (例如: ==, >, <, >=, <=)
    ComparisonOperator comparison_type;
    
    //! 具体的常量值
    Scalar constant;
};

}