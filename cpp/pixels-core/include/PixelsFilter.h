#ifndef DUCKDB_PIXELSFILTER_H
#define DUCKDB_PIXELSFILTER_H

#include <bitset>
#include "PixelsBitMask.h"
#include "vector/ColumnVector.h"
#include "TypeDescription.h"
#include <immintrin.h>
#include <avxintrin.h>
#include "filter/table_filter.hpp"
#include "filter/constant_filter.hpp"
#include "filter/conjunction_filter.hpp"
#include "filter/comparison_operators.hpp"

#define ENABLE_SIMD_FILTER

class PixelsFilter
{
public:
    static void ApplyFilter(std::shared_ptr <ColumnVector> vector, const pixels::TableFilter &filter,
                            PixelsBitMask &filterMask,
                            std::shared_ptr <TypeDescription> type);

    template<class T, class OP>
    static int CompareAvx2(void *data, T constant);

    /*template<class T, class OP>
    static void TemplatedFilterOperation(std::shared_ptr <ColumnVector> vector,
                                         const pixels::Scalar &constant, PixelsBitMask &filter_mask,
                                         std::shared_ptr <TypeDescription> type);*/
    template<class OP>
    static void IntFilterOperation(std::shared_ptr <ColumnVector> vector,
                                    const pixels::Scalar &constant, PixelsBitMask &filter_mask);

    template<class OP>
    static void LongFilterOperation(std::shared_ptr <ColumnVector> vector,
                                    const pixels::Scalar &constant, PixelsBitMask &filter_mask);
    template<class OP>
    static void DateFilterOperation(std::shared_ptr <ColumnVector> vector,
                                    const pixels::Scalar &constant, PixelsBitMask &filter_mask);
    
    template<class OP>
    static void DecimalFilterOperation(std::shared_ptr <ColumnVector> vector,
                                    const pixels::Scalar &constant, PixelsBitMask &filter_mask);    
    
    template<class OP>
    static void StringFilterOperation(std::shared_ptr <ColumnVector> vector,
                                    const pixels::Scalar &constant, PixelsBitMask &filter_mask);                                    

    template<class OP>
    static void FilterOperationSwitch(std::shared_ptr <ColumnVector> vector, const pixels::Scalar &constant,
                                      PixelsBitMask &filter_mask, std::shared_ptr <TypeDescription> type);

};
#endif //DUCKDB_PIXELSFILTER_H