project(pixels-core)

set(pixels_core_cxx
        lib/PixelsVersion.cpp
        lib/PixelsFooterCache.cpp
        lib/exception/PixelsReaderException.cpp
        lib/exception/PixelsFileMagicInvalidException.cpp
        lib/exception/PixelsFileVersionInvalidException.cpp
        lib/TypeDescription.cpp
        lib/Category.cpp
        lib/vector/LongColumnVector.cpp
        lib/vector/VectorizedRowBatch.cpp
        lib/vector/ColumnVector.cpp
        lib/vector/ByteColumnVector.cpp
        lib/vector/BinaryColumnVector.cpp
        lib/encoding/Decoder.cpp
        lib/encoding/RunLenIntDecoder.cpp
        lib/encoding/Encoder.cpp
        lib/encoding/RunLenIntEncoder.cpp
        lib/encoding/EncodingLevel.cpp
        lib/utils/EncodingUtils.cpp
        lib/utils/EncodingUtils.cpp
        include/vector/DecimalColumnVector.h
        lib/vector/DecimalColumnVector.cpp
        lib/vector/DateColumnVector.cpp
        include/vector/DateColumnVector.h
        include/vector/TimestampColumnVector.h
        lib/vector/TimestampColumnVector.cpp
        lib/writer/ColumnWriter.cpp
        lib/writer/PixelsWriterOption.cpp
        lib/encoding/EncodingLevel.cpp
        lib/PixelsWriterImpl.cpp
        lib/stats/StatsRecorder.cpp
        include/utils/BitUtils.h
        lib/utils/BitUtils.cpp
        include/writer/ColumnWriterBuilder.h
        lib/writer/ColumnWriterBuilder.cpp
        include/writer/IntegerColumnWriter.h
        lib/writer/IntegerColumnWriter.cpp
        include/writer/testColumnWriter.h
        lib/writer/testColumnWriter.cpp
        lib/writer/testColumnWriter.cpp
)

add_library(pixels-core ${pixels_core_cxx})

target_link_libraries(
        pixels-core
        pixels-common
)
SET(CMAKE_CXX_FLAGS "-mavx2")

include_directories(${CMAKE_CURRENT_BINARY_DIR}/../pixels-common/liburing/src/include)
include_directories(../pixels-common/include)
include_directories(include)
include_directories(include/writer)
