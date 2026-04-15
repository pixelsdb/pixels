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
    //std::cout<<"enter compare"<<std::endl;
    __m256i vector;
    __m256i vector_next;
    __m256i constants;
    __m256i mask;
    if constexpr(sizeof(T) == 4)
    {
        vector = _mm256_load_si256((__m256i *) data);
        constants = _mm256_set1_epi32(constant);
        if constexpr(std::is_same<OP, pixels::Equals>())
        {
            mask = _mm256_cmpeq_epi32(vector, constants);
            return _mm256_movemask_ps((__m256) mask);
        } else if constexpr(std::is_same<OP, pixels::LessThan>())
        {
            mask = _mm256_cmpgt_epi32(constants, vector);
            return _mm256_movemask_ps((__m256) mask);
        } else if constexpr(std::is_same<OP, pixels::LessThanEquals>())
        {
            mask = _mm256_cmpgt_epi32(vector, constants);
            return ~_mm256_movemask_ps((__m256) mask);
        } else if constexpr(std::is_same<OP, pixels::GreaterThan>())
        {
            mask = _mm256_cmpgt_epi32(vector, constants);
            return _mm256_movemask_ps((__m256) mask);
        } else if constexpr(std::is_same<OP, pixels::GreaterThanEquals>())
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
        if constexpr(std::is_same<OP, pixels::Equals>())
        {
            mask = _mm256_cmpeq_epi64(vector, constants);
            result = _mm256_movemask_pd((__m256d) mask);
            mask = _mm256_cmpeq_epi64(vector_next, constants);
            result += _mm256_movemask_pd((__m256d) mask) << 4;
            return result;
        } else if constexpr(std::is_same<OP, pixels::LessThan>())
        {
            mask = _mm256_cmpgt_epi64(constants, vector);
            result = _mm256_movemask_pd((__m256d) mask);
            mask = _mm256_cmpgt_epi64(constants, vector_next);
            result += _mm256_movemask_pd((__m256d) mask) << 4;
            return result;
        } else if constexpr(std::is_same<OP, pixels::LessThanEquals>())
        {
            mask = _mm256_cmpgt_epi64(vector, constants);
            result = _mm256_movemask_pd((__m256d) mask);
            mask = _mm256_cmpgt_epi64(vector_next, constants);
            result += _mm256_movemask_pd((__m256d) mask) << 4;
            return ~result;
        } else if constexpr(std::is_same<OP, pixels::GreaterThan>())
        {
            mask = _mm256_cmpgt_epi64(vector, constants);
            result = _mm256_movemask_pd((__m256d) mask);
            mask = _mm256_cmpgt_epi64(vector_next, constants);
            result += _mm256_movemask_pd((__m256d) mask) << 4;
            return result;
        } else if constexpr(std::is_same<OP, pixels::GreaterThanEquals>())
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

template<class OP>
void PixelsFilter::IntFilterOperation(std::shared_ptr <ColumnVector> vector,
                                    const pixels::Scalar &constant, PixelsBitMask &filter_mask){
    int32_t constant_value=constant.get_int32();
    auto intColumnVector = std::static_pointer_cast<IntColumnVector>(vector);
    int i = 0;
#ifdef  ENABLE_SIMD_FILTER
    for (; i < vector->length - vector->length % 8; i += 8) {
        uint8_t mask = CompareAvx2<int32_t, OP>(intColumnVector->intVector + i, constant_value);
        filter_mask.setByteAligned(i, mask);
    }
#endif
    for (; i < vector->length; i++)
    {
        filter_mask.set(i, OP::Operation((int32_t) intColumnVector->intVector[i],constant_value));
    }
}

template<class OP>
void PixelsFilter::LongFilterOperation(std::shared_ptr <ColumnVector> vector,const pixels::Scalar &constant, PixelsBitMask &filter_mask){
    int64_t constant_value=constant.get_int64(); 
    auto longColumnVector = std::static_pointer_cast<LongColumnVector>(vector);
    int i = 0;
#ifdef ENABLE_SIMD_FILTER
    for (; i < vector->length - vector->length % 8; i += 8) {
        uint8_t mask = CompareAvx2<int64_t, OP>(longColumnVector->longVector + i, constant_value);
        filter_mask.setByteAligned(i,mask);

    }
#endif
    for (; i < vector->length; i++)
    {
        filter_mask.set(i, OP::Operation((int64_t) longColumnVector->longVector[i],constant_value));
    }
}

template<class OP>
void PixelsFilter::DateFilterOperation(std::shared_ptr <ColumnVector> vector,
                                    const pixels::Scalar &constant, PixelsBitMask &filter_mask){
    int32_t constant_value=constant.get_int32(); //date value is also represented as int32 internally
    auto dateColumnVector = std::static_pointer_cast<DateColumnVector>(vector);
    int i = 0;
#ifdef ENABLE_SIMD_FILTER
    for (; i < vector->length - vector->length % 8; i += 8) {
        uint8_t mask = CompareAvx2<int32_t, OP>(dateColumnVector->dates + i, constant_value);
        filter_mask.setByteAligned(i, mask);
    }
#endif
    for (; i < vector->length; i++)
    {
        filter_mask.set(i, OP::Operation((int32_t) dateColumnVector->dates[i],constant_value));
    }
}

template<class OP>
void PixelsFilter::DecimalFilterOperation(std::shared_ptr <ColumnVector> vector,
                                    const pixels::Scalar &constant, PixelsBitMask &filter_mask){
    int64_t constant_value=constant.get_int64(); //
    auto decimalColumnVector = std::static_pointer_cast<DecimalColumnVector>(vector);
    int i = 0;
#ifdef ENABLE_SIMD_FILTER
    for (; i < vector->length - vector->length % 8; i += 8) {
        uint8_t mask = CompareAvx2<int64_t, OP>(decimalColumnVector->vector + i, constant_value);
        filter_mask.setByteAligned(i, mask);
    }
#endif
    for (; i < vector->length; i++)
    {
        filter_mask.set(i,OP::Operation((int64_t)((int64_t*)decimalColumnVector->vector)[i], constant_value));
    }                                    
}

template<class OP>
void PixelsFilter::StringFilterOperation(std::shared_ptr <ColumnVector> vector,//no support for SIMD filter for string for now, 
                                    const pixels::Scalar &constant, PixelsBitMask &filter_mask){
    std::string constant_value=constant.get_string();
    auto binaryColumnVector = std::static_pointer_cast<BinaryColumnVector>(vector);
    for (int i = 0; i < vector->length; i++)
    {
        if(!vector->checkValid(i)){//NULL
            filter_mask.set(i,0);
        }
        if(filter_mask.get(i))
            filter_mask.set(i, OP::Operation(binaryColumnVector->vector[i].ToString(),constant_value));
    }
}       

/*template<class T, class OP>
void PixelsFilter::TemplatedFilterOperation(std::shared_ptr <ColumnVector> vector,
                                            const pixels::Scalar &constant, PixelsBitMask &filter_mask,
                                            std::shared_ptr <TypeDescription> type)
{
    T constant_value;
    if (auto ptr = std::get_if<T>(&constant)) {
        constant_value = *ptr;
        std::cout << "value is " << constant_value << std::endl;
    } else {
            std::cerr << "Invalid type in constant" << std::endl;
    }

    switch (type->getCategory())
    {
        case TypeDescription::SHORT:
        case TypeDescription::INT:
        {
            std::cout<<"it is int"<<std::endl;
            auto intColumnVector = std::static_pointer_cast<IntColumnVector>(vector);
            int i = 0;
#ifdef  ENABLE_SIMD_FILTER
            for (; i < vector->length - vector->length % 8; i += 8) {
                uint8_t mask = CompareAvx2<T, OP>(intColumnVector->intVector + i, constant_value);
                filter_mask.setByteAligned(i, mask);
            }
#endif
            for (; i < vector->length; i++)
            {
                filter_mask.set(i, OP::Operation((T) intColumnVector->intVector[i],
                                                 constant_value));
            }
            break;
        }
        case TypeDescription::LONG:
        {
            std::cout<<"it is long"<<std::endl;
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
                filter_mask.set(i,OP::Operation((T)((int64_t*)decimalColumnVector->vector)[i], constant_value));
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
                filter_mask.set(i, OP::Operation((std::string) binaryColumnVector->vector[i],
                                                 (std::string) constant_value));
            }
            break;
        }
    }
}*/

template<class OP>
void PixelsFilter::FilterOperationSwitch(std::shared_ptr <ColumnVector> vector, const pixels::Scalar &constant,
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
            IntFilterOperation<OP>(vector,constant,filter_mask);
            break;
        case TypeDescription::DATE:
            DateFilterOperation<OP>(vector, constant, filter_mask);
            break;
        case TypeDescription::LONG:
            LongFilterOperation<OP>(vector, constant, filter_mask);
            break;
        case TypeDescription::DECIMAL:
            DecimalFilterOperation<OP>(vector, constant, filter_mask);
            break;
        case TypeDescription::STRING:
        case TypeDescription::BINARY:
        case TypeDescription::VARBINARY:
        case TypeDescription::CHAR:
        case TypeDescription::VARCHAR:
            StringFilterOperation<OP>(vector, constant, filter_mask);
            break;
        default:
            throw InvalidArgumentException("Unsupported type for filter. ");
    }
}

void PixelsFilter::ApplyFilter(std::shared_ptr <ColumnVector> vector, const pixels::TableFilter &filter,
                               PixelsBitMask &filterMask,
                               std::shared_ptr <TypeDescription> type)
{
    
    switch (filter.filter_type)
    {
        //std::cout<<(int)filter.filter_type<<std::endl;
        case pixels::TableFilterType::CONJUNCTION_AND:
        {
            //std::cout<<"this a conjuntionand filter"<<std::endl;
            auto &conjunction = (pixels::ConjunctionAndFilter &) filter;
            for (auto &child_filter: conjunction.child_filters)
            {
                PixelsBitMask childMask(filterMask.maskLength);
                ApplyFilter(vector, *child_filter, childMask, type);
                filterMask.And(childMask);
            }
            break;
        }
        case pixels::TableFilterType::CONJUNCTION_OR:
        {
            //std::cout<<"this a conjuntionor filter"<<std::endl;
            auto &conjunction = (pixels::ConjunctionOrFilter &) filter;
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
        case pixels::TableFilterType::CONSTANT_COMPARISON:
        {
            auto &constant_filter = (pixels::ConstantFilter &) filter;
            switch (constant_filter.comparison_type)
            {
                case pixels::ComparisonOperator::EQUAL:
                    FilterOperationSwitch<pixels::Equals>(
                            vector,constant_filter.constant, filterMask, type);
                    break;
                case pixels::ComparisonOperator::LESS_THAN:
                    FilterOperationSwitch<pixels::LessThan>(
                            vector, constant_filter.constant, filterMask, type);
                    break;
                case pixels::ComparisonOperator::LESS_THAN_OR_EQUAL:
                    FilterOperationSwitch<pixels::LessThanEquals>(
                            vector, constant_filter.constant, filterMask, type);
                    break;
                case pixels::ComparisonOperator::GREATER_THAN:
                    FilterOperationSwitch<pixels::GreaterThan>(
                            vector, constant_filter.constant, filterMask, type);
                    break;
                case pixels::ComparisonOperator::GREATER_THAN_OR_EQUAL:
                    FilterOperationSwitch<pixels::GreaterThanEquals>(
                            vector, constant_filter.constant, filterMask, type);
                    break;
                default:
                    //D_ASSERT(0);do nothing for unsupported comparison operator for now
                    break;
            }
            break;
        }
        case pixels::TableFilterType::IS_NOT_NULL:
            // TODO: support is null
            break;
        case pixels::TableFilterType::IS_NULL:
            // TODO: support is null
            break;
        case pixels::TableFilterType::OPTIONAL_FILTER:
            // nothing to do
            return;
        default:
            //D_ASSERT(0); do nothing for unsupported filter type for now
            break;
    }
}