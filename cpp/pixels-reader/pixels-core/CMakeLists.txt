project(pixels-core)

set(pixels_core_cxx
        lib/PixelsReaderImpl.cpp
        lib/PixelsReaderBuilder.cpp
        lib/reader/PixelsRecordReaderImpl.cpp
        lib/PixelsVersion.cpp
        lib/PixelsFooterCache.cpp
        lib/exception/PixelsReaderException.cpp
        lib/exception/PixelsFileMagicInvalidException.cpp
        lib/exception/PixelsFileVersionInvalidException.cpp
        lib/reader/PixelsReaderOption.cpp
        lib/TypeDescription.cpp
        lib/Category.cpp
        lib/vector/LongColumnVector.cpp
        lib/vector/VectorizedRowBatch.cpp
        lib/vector/ColumnVector.cpp
        lib/vector/ByteColumnVector.cpp
        lib/vector/BinaryColumnVector.cpp lib/reader/ColumnReader.cpp lib/reader/ColumnReaderBuilder.cpp lib/reader/IntegerColumnReader.cpp lib/reader/CharColumnReader.cpp lib/reader/VarcharColumnReader.cpp lib/reader/StringColumnReader.cpp lib/encoding/Decoder.cpp lib/encoding/RunLenIntDecoder.cpp lib/encoding/Encoder.cpp lib/encoding/RunLenIntEncoder.cpp lib/utils/EncodingUtils.cpp lib/utils/EncodingUtils.cpp include/vector/DecimalColumnVector.h lib/vector/DecimalColumnVector.cpp include/reader/DecimalColumnReader.h lib/reader/DecimalColumnReader.cpp lib/vector/DateColumnVector.cpp include/vector/DateColumnVector.h include/reader/DateColumnReader.h lib/reader/DateColumnReader.cpp include/PixelsFilter.h lib/PixelsFilter.cpp
        include/PixelsFilterMask.h
        lib/PixelsFilterMask.cpp)

add_library(pixels-core ${pixels_core_cxx})

target_link_libraries(
        pixels-core
        pixels-common
)
SET(CMAKE_CXX_FLAGS "-mavx2")

include_directories(${CMAKE_CURRENT_BINARY_DIR}/../pixels-common/liburing/src/include)
include_directories(../pixels-common/include)
include_directories(include)
