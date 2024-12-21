/*
 * Copyright 2023 PixelsDB.
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
 * @author liyu
 * @create 2023-06-23
 */
#include "PixelsFilter.h"

template<class T, class OP>
int PixelsFilter::CompareAvx2(void *data, T constant)
{
    __m256i vector;
    __m256i vector_next;
    __m256i constants;
    __m256i mask;
    if constexpr(sizeof(T) == 4)
    {
        vector = _mm256_load_si256((__m256i *) data);
        constants = _mm256_set1_epi32(constant);
        if constexpr(std::is_same<OP, duckdb::Equals>())
        {
            mask = _mm256_cmpeq_epi32(vector, constants);
            return _mm256_movemask_ps((__m256) mask);
        } else if constexpr(std::is_same<OP, duckdb::LessThan>())
        {
            mask = _mm256_cmpgt_epi32(constants, vector);
            return _mm256_movemask_ps((__m256) mask);
        } else if constexpr(std::is_same<OP, duckdb::LessThanEquals>())
        {
            mask = _mm256_cmpgt_epi32(vector, constants);
            return ~_mm256_movemask_ps((__m256) mask);
        } else if constexpr(std::is_same<OP, duckdb::GreaterThan>())
        {
            mask = _mm256_cmpgt_epi32(vector, constants);
            return _mm256_movemask_ps((__m256) mask);
        } else if constexpr(std::is_same<OP, duckdb::GreaterThanEquals>())
        {
            mask = _mm256_cmpgt_epi32(constants, vector);
            return ~_mm256_movemask_ps((__m256) mask);
        }
    } else if constexpr(sizeof(T) == 8)
    {
        constants = _mm256_set1_epi64x(constant);
        vector = _mm256_load_si256((__m256i *) data);
        vector_next = _mm256_load_si256((__m256i * )((uint8_t *) data + 32));
        int result = 0;
        if constexpr(std::is_same<OP, duckdb::Equals>())
        {
            mask = _mm256_cmpeq_epi64(vector, constants);
            result = _mm256_movemask_pd((__m256d) mask);
            mask = _mm256_cmpeq_epi64(vector_next, constants);
            result += _mm256_movemask_pd((__m256d) mask) << 4;
            return result;
        } else if constexpr(std::is_same<OP, duckdb::LessThan>())
        {
            mask = _mm256_cmpgt_epi64(constants, vector);
            result = _mm256_movemask_pd((__m256d) mask);
            mask = _mm256_cmpgt_epi64(constants, vector_next);
            result += _mm256_movemask_pd((__m256d) mask) << 4;
            return result;
        } else if constexpr(std::is_same<OP, duckdb::LessThanEquals>())
        {
            mask = _mm256_cmpgt_epi64(vector, constants);
            result = _mm256_movemask_pd((__m256d) mask);
            mask = _mm256_cmpgt_epi64(vector_next, constants);
            result += _mm256_movemask_pd((__m256d) mask) << 4;
            return ~result;
        } else if constexpr(std::is_same<OP, duckdb::GreaterThan>())
        {
            mask = _mm256_cmpgt_epi64(vector, constants);
            result = _mm256_movemask_pd((__m256d) mask);
            mask = _mm256_cmpgt_epi64(vector_next, constants);
            result += _mm256_movemask_pd((__m256d) mask) << 4;
            return result;
        } else if constexpr(std::is_same<OP, duckdb::GreaterThanEquals>())
        {
            mask = _mm256_cmpgt_epi64(constants, vector);
            result = _mm256_movemask_pd((__m256d) mask);
            mask = _mm256_cmpgt_epi64(constants, vector_next);
            result += _mm256_movemask_pd((__m256d) mask) << 4;
            return ~result;
        }
    } else {
        throw InvalidArgumentException("We didn't support other sizes yet to do filter SIMD");
    }
}


template<class T, class OP>
void PixelsFilter::TemplatedFilterOperation(std::shared_ptr <ColumnVector> vector,
                                            const duckdb::Value &constant, PixelsBitMask &filter_mask,
                                            std::shared_ptr <TypeDescription> type)
{
    T constant_value = constant.template GetValueUnsafe<T>();
    switch (type->getCategory())
    {
        case TypeDescription::SHORT:
        case TypeDescription::INT:
        {
            auto longColumnVector = std::static_pointer_cast<LongColumnVector>(vector);
            int i = 0;
#ifdef  ENABLE_SIMD_FILTER
            for (; i < vector->length - vector->length % 8; i += 8) {
                uint8_t mask = CompareAvx2<T, OP>(longColumnVector->intVector + i, constant_value);
                filter_mask.setByteAligned(i, mask);
            }
#endif
            for (; i < vector->length; i++)
            {
                filter_mask.set(i, OP::Operation((T) longColumnVector->intVector[i],
                                                 constant_value));
            }
            break;
        }
        case TypeDescription::LONG:
        {
            auto longColumnVector = std::static_pointer_cast<LongColumnVector>(vector);
            int i = 0;
#ifdef ENABLE_SIMD_FILTER
            for (; i < vector->length - vector->length % 8; i += 8) {
                uint8_t mask = CompareAvx2<T, OP>(longColumnVector->longVector + i, constant_value);
                filter_mask.setByteAligned(i, mask);
            }
#endif
            for (; i < vector->length; i++)
            {
                filter_mask.set(i, OP::Operation((T) longColumnVector->longVector[i],
                                                 constant_value));
            }
            break;
        }
        case TypeDescription::DATE:
        {
            auto dateColumnVector = std::static_pointer_cast<DateColumnVector>(vector);
            int i = 0;
#ifdef ENABLE_SIMD_FILTER
            for (; i < vector->length - vector->length % 8; i += 8) {
                uint8_t mask = CompareAvx2<T, OP>(dateColumnVector->dates + i, constant_value);
                filter_mask.setByteAligned(i, mask);
            }
#endif
            for (; i < vector->length; i++)
            {
                filter_mask.set(i, OP::Operation((T) dateColumnVector->dates[i],
                                                 constant_value));
            }
            break;
        }
        case TypeDescription::DECIMAL:
        {
            auto decimalColumnVector = std::static_pointer_cast<DecimalColumnVector>(vector);
            int i = 0;
#ifdef ENABLE_SIMD_FILTER
            for (; i < vector->length - vector->length % 8; i += 8) {
                uint8_t mask = CompareAvx2<T, OP>(decimalColumnVector->vector + i, constant_value);
                filter_mask.setByteAligned(i, mask);
            }
#endif
            for (; i < vector->length; i++)
            {
                filter_mask.set(i, OP::Operation((T) decimalColumnVector->vector[i],
                                                 constant_value));
            }
            break;
        }
        case TypeDescription::STRING:
        case TypeDescription::BINARY:
        case TypeDescription::VARBINARY:
        case TypeDescription::CHAR:
        case TypeDescription::VARCHAR:
        {
            auto binaryColumnVector = std::static_pointer_cast<BinaryColumnVector>(vector);
            for (int i = 0; i < vector->length; i++)
            {
                filter_mask.set(i, OP::Operation((duckdb::string_t) binaryColumnVector->vector[i],
                                                 (duckdb::string_t) constant_value));
            }
            break;
        }
    }
}

template<class OP>
void PixelsFilter::FilterOperationSwitch(std::shared_ptr <ColumnVector> vector, duckdb::Value &constant,
                                         PixelsBitMask &filter_mask,
                                         std::shared_ptr <TypeDescription> type)
{
    if (filter_mask.isNone())
    {
        return;
    }
    switch (type->getCategory())
    {
        case TypeDescription::SHORT:
        case TypeDescription::INT:
        case TypeDescription::DATE:
            TemplatedFilterOperation<int32_t, OP>(vector, constant, filter_mask, type);
            break;
        case TypeDescription::LONG:
            TemplatedFilterOperation<int64_t, OP>(vector, constant, filter_mask, type);
            break;
        case TypeDescription::DECIMAL:
            TemplatedFilterOperation<int64_t, OP>(vector, constant, filter_mask, type);
            break;
        case TypeDescription::STRING:
        case TypeDescription::BINARY:
        case TypeDescription::VARBINARY:
        case TypeDescription::CHAR:
        case TypeDescription::VARCHAR:
            TemplatedFilterOperation<duckdb::string_t, OP>(vector, constant, filter_mask, type);
            break;
        default:
            throw InvalidArgumentException("Unsupported type for filter. ");
    }
}

void PixelsFilter::ApplyFilter(std::shared_ptr <ColumnVector> vector, duckdb::TableFilter &filter,
                               PixelsBitMask &filterMask,
                               std::shared_ptr <TypeDescription> type)
{
    switch (filter.filter_type)
    {
        case duckdb::TableFilterType::CONJUNCTION_AND:
        {
            auto &conjunction = (duckdb::ConjunctionAndFilter &) filter;
            for (auto &child_filter: conjunction.child_filters)
            {
                PixelsBitMask childMask(filterMask.maskLength);
                ApplyFilter(vector, *child_filter, childMask, type);
                filterMask.And(childMask);
            }
            break;
        }
        case duckdb::TableFilterType::CONJUNCTION_OR:
        {
            auto &conjunction = (duckdb::ConjunctionOrFilter &) filter;
            PixelsBitMask orMask(filterMask.maskLength);
            for (auto &childFilter: conjunction.child_filters)
            {
                PixelsBitMask childMask(filterMask);
                ApplyFilter(vector, *childFilter, childMask, type);
                orMask.Or(childMask);
            }
            filterMask.And(orMask);
            break;
        }
        case duckdb::TableFilterType::CONSTANT_COMPARISON:
        {
            auto &constant_filter = (duckdb::ConstantFilter &) filter;
            switch (constant_filter.comparison_type)
            {
                case duckdb::ExpressionType::COMPARE_EQUAL:
                    FilterOperationSwitch<duckdb::Equals>(
                            vector, constant_filter.constant, filterMask, type);
                    break;
                case duckdb::ExpressionType::COMPARE_LESSTHAN:
                    FilterOperationSwitch<duckdb::LessThan>(
                            vector, constant_filter.constant, filterMask, type);
                    break;
                case duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO:
                    FilterOperationSwitch<duckdb::LessThanEquals>(
                            vector, constant_filter.constant, filterMask, type);
                    break;
                case duckdb::ExpressionType::COMPARE_GREATERTHAN:
                    FilterOperationSwitch<duckdb::GreaterThan>(
                            vector, constant_filter.constant, filterMask, type);
                    break;
                case duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO:
                    FilterOperationSwitch<duckdb::GreaterThanEquals>(
                            vector, constant_filter.constant, filterMask, type);
                    break;
                default:
                    D_ASSERT(0);
            }
            break;
        }
        case duckdb::TableFilterType::IS_NOT_NULL:
            // TODO: support is null
            break;
        case duckdb::TableFilterType::IS_NULL:
            // TODO: support is null
            break;
        default:
            D_ASSERT(0);
            break;
    }
}




