#include "writer/IntegerColumnWriter.h"
#include "reader/IntegerColumnReader.h"
#include "vector/LongColumnVector.h"

#include "gtest/gtest.h"


TEST(IntegerWriterTest, WriteRunLengthEncodeIntWithoutNull) {
        int len = 10;
        int pixel_stride = 5;
        bool is_long = false;
        bool encoding = true;
        auto integer_column_vector = std::make_shared<LongColumnVector>(len, encoding, is_long);
        ASSERT_TRUE(integer_column_vector);
        for (int i = 0; i < len; ++i)
        {
            integer_column_vector->add(i);
        }
        /**----------------------------------------------
         * *                   INFO
         *   Case1: RunLengthEncode
         *   
         *---------------------------------------------**/
        auto option = std::make_shared<PixelsWriterOption>();
        option->setPixelsStride(pixel_stride);
        option->setNullsPadding(false);
        option->setEncodingLevel(EncodingLevel(EncodingLevel::EL2));

        auto integer_column_writer = std::make_unique<IntegerColumnWriter>
            (TypeDescription::createInt(), option);
        auto write_size = integer_column_writer->write(integer_column_vector, len);
        EXPECT_NE(write_size, 0);
        integer_column_writer->flush();
        auto content = integer_column_writer->getColumnChunkContent();
        EXPECT_GT(content.size(), 0);

       
        std::cerr << "[DEBUG] content size: " << content.size() << std::endl;
        integer_column_writer->close();

        /**----------------------
         **      Write End. Use Reader to check
         *------------------------**/
        auto integer_column_reader = std::make_unique<IntegerColumnReader>(TypeDescription::createInt());
        auto buffer = std::make_shared<ByteBuffer>(content.size());
        buffer->putBytes(content.data(), content.size());
        auto column_chunk_encoding = integer_column_writer->getColumnChunkEncoding();
        auto int_result_vector = std::make_shared<LongColumnVector>(len, encoding, is_long);
        auto bit_mask = std::make_shared<PixelsBitMask>(len);


        auto num_to_read = len;
        auto pixel_offset = 0;
        auto vector_index = 0;
        while(num_to_read) {
            auto size = std::min(pixel_stride, num_to_read);
            integer_column_reader->read(buffer, column_chunk_encoding, pixel_offset, size, pixel_stride, vector_index,
                int_result_vector,
                *integer_column_writer->getColumnChunkIndexPtr(),
                bit_mask);
            for(int i = vector_index; i < vector_index + size; i++) {
                std::cerr << "[DEBUG READ CASE1] " << int_result_vector->intVector[i] << std::endl;
                EXPECT_EQ(int_result_vector->intVector[i], integer_column_vector->intVector[i]);
            }
            pixel_offset+=size;
            vector_index+=size;
            num_to_read-=size;
        }
}

TEST(IntegerWriterTest, DISABLED_WriteIntWithoutNull) {
        int len = 10;
        int pixel_stride = 5;
        bool is_long = false;
        bool encoding = false;
        auto integer_column_vector = std::make_shared<LongColumnVector>(len, encoding, is_long);
        ASSERT_TRUE(integer_column_vector);
        for (int i = 0; i < len; ++i)
        {
            integer_column_vector->add(i);
        }
        /**----------------------------------------------
         * *                   INFO
         *   Case2: Without RunLengthEncode
         *   
         *---------------------------------------------**/
        auto option = std::make_shared<PixelsWriterOption>();
        option->setPixelsStride(pixel_stride);
        option->setNullsPadding(false);
        option->setByteOrder(ByteOrder::PIXELS_LITTLE_ENDIAN);
        option->setEncodingLevel(EncodingLevel(EncodingLevel::EL0));

        auto integer_column_writer = std::make_unique<IntegerColumnWriter>
            (TypeDescription::createInt(), option);
        auto write_size = integer_column_writer->write(integer_column_vector, len);
        EXPECT_NE(write_size, 0);
        integer_column_writer->flush();
        auto content = integer_column_writer->getColumnChunkContent();
        EXPECT_GT(content.size(), 0);

       
        std::cerr << "[DEBUG] content size: " << content.size() << std::endl;
        integer_column_writer->close();

        /**----------------------
         **      Write End. Use Reader to check
         *------------------------**/
        auto integer_column_reader = std::make_unique<IntegerColumnReader>(TypeDescription::createInt());
        auto buffer = std::make_shared<ByteBuffer>(content.size());
        buffer->putBytes(content.data(), content.size());
        auto column_chunk_encoding = integer_column_writer->getColumnChunkEncoding();
        auto int_result_vector = std::make_shared<LongColumnVector>(len, encoding, is_long);
        auto bit_mask = std::make_shared<PixelsBitMask>(len);


        auto num_to_read = len;
        auto pixel_offset = 0;
        auto vector_index = 0;
        while(num_to_read) {
            auto size = std::min(pixel_stride, num_to_read);
            integer_column_reader->read(buffer, column_chunk_encoding, pixel_offset, size, pixel_stride, vector_index,
                int_result_vector,
                *integer_column_writer->getColumnChunkIndexPtr(),
                bit_mask);
            for(int i = vector_index; i < vector_index + size; i++) {
                std::cerr << "[DEBUG READ CASE1] " << reinterpret_cast<int*>(int_result_vector->intVector)[i] << std::endl;
                EXPECT_EQ(reinterpret_cast<int*>(int_result_vector->intVector)[i], integer_column_vector->intVector[i]);
            }
            pixel_offset+=size;
            vector_index+=size;
            num_to_read-=size;
        }
}

TEST(EncodeTest, DISABLED_EncodeLong) {
    const size_t len = 10;
    std::array<long, len> data;
    for(int i = 0; i < len; i++) {
        data[i] = INT64_MAX - i;
    }
    auto encode_buffer = std::make_shared<ByteBuffer>();
    auto encoder = std::make_unique<RunLenIntEncoder>();

    int res_len{0};
    encoder->encode(data.data(), encode_buffer->getPointer(), len, res_len);

    EXPECT_GT(res_len, 0);

    bool is_signed = true;
    auto decoder = std::make_unique<RunLenIntDecoder>(encode_buffer, is_signed);
    for(int i = 0; i < len; i++) {
        EXPECT_EQ(decoder->next(), data[i]);
    }
}


TEST(IntegerWriterTest, DISABLED_WriteRunLengthEncodeLongWithoutNull) {
        int len = 23;
        int pixel_stride = 5;
        bool is_long = true;
        bool encoding = true;
        auto long_column_vector = std::make_shared<LongColumnVector>(len, encoding, is_long);
        ASSERT_TRUE(long_column_vector);
        for (long i = 0; i < len; ++i)
        {
            long_column_vector->add(INT64_MAX - i);
        }
        // Run Length Encoding
        auto option = std::make_shared<PixelsWriterOption>();
        option->setPixelsStride(pixel_stride);
        option->setNullsPadding(false);
        option->setEncodingLevel(EncodingLevel(EncodingLevel::EL2));

        auto long_column_writer = std::make_unique<IntegerColumnWriter>
            (TypeDescription::createLong(), option);
        auto write_size = long_column_writer->write(long_column_vector, len);
        EXPECT_NE(write_size, 0);
        long_column_writer->flush();
        auto content = long_column_writer->getColumnChunkContent();
        EXPECT_GT(content.size(), 0);
        std::cerr << "[DEBUG] content size: " << content.size() << std::endl;


        /**----------------------
         **      Write End. Use Reader to check
         *------------------------**/
        auto long_column_reader = std::make_unique<IntegerColumnReader>(TypeDescription::createLong());
        auto buffer = std::make_shared<ByteBuffer>(content.size());
        buffer->putBytes(content.data(), content.size());
        auto column_chunk_encoding = long_column_writer->getColumnChunkEncoding();
        auto long_result_vector = std::make_shared<LongColumnVector>(len, encoding, is_long);
        auto bit_mask = std::make_shared<PixelsBitMask>(len);
        auto num_to_read = len;
        auto pixel_offset = 0;
        auto vector_index = 0;
        while(num_to_read) {
            auto size = std::min(pixel_stride, num_to_read);
            long_column_reader->read(buffer, column_chunk_encoding, pixel_offset, size, pixel_stride, vector_index,
                long_result_vector,
                *(long_column_writer->getColumnChunkIndexPtr()),
                bit_mask);
            for(int i = vector_index; i < vector_index + size; i++) {
                std::cerr << "[DEBUG READ CASE1] " << long_result_vector->longVector[i] << std::endl;
                EXPECT_EQ(long_result_vector->longVector[i], long_column_vector->longVector[i]);
            }
            pixel_offset+=size;
            vector_index+=size;
            num_to_read-=size;
        }

        long_column_writer->close();

}

TEST(IntegerWriterTest, DISABLED_WriteRunLengthEncodeIntWithNull) {
        int len = 23;
        int pixel_stride = 5;
        bool is_long = false;
        bool encoding = true;
        auto integer_column_vector = std::make_shared<LongColumnVector>(len, encoding, is_long);
        ASSERT_TRUE(integer_column_vector);
        for (int i = 0; i < len; ++i)
        {
            if(i%2) {
                integer_column_vector->add(i);
            } else {
                integer_column_vector->addNull();
            }

        }
        /**----------------------------------------------
         * *                   INFO
         *   Case: RunLengthEncode
         *   
         *---------------------------------------------**/
        auto option = std::make_shared<PixelsWriterOption>();
        option->setPixelsStride(pixel_stride);
        option->setNullsPadding(false);
        option->setEncodingLevel(EncodingLevel(EncodingLevel::EL2));

        auto integer_column_writer = std::make_unique<IntegerColumnWriter>
            (TypeDescription::createInt(), option);
        auto write_size = integer_column_writer->write(integer_column_vector, len);
        EXPECT_NE(write_size, 0);
        integer_column_writer->flush();
        auto content = integer_column_writer->getColumnChunkContent();
        EXPECT_GT(content.size(), 0);

       
        std::cerr << "[DEBUG] content size: " << content.size() << std::endl;
        integer_column_writer->close();

        /**----------------------
         **      Write End. Use Reader to check
         *------------------------**/
        auto integer_column_reader = std::make_unique<IntegerColumnReader>(TypeDescription::createInt());
        auto buffer = std::make_shared<ByteBuffer>(content.size());
        buffer->putBytes(content.data(), content.size());
        auto column_chunk_encoding = integer_column_writer->getColumnChunkEncoding();
        auto int_result_vector = std::make_shared<LongColumnVector>(len, encoding, is_long);
        auto bit_mask = std::make_shared<PixelsBitMask>(len);


        auto num_to_read = len;
        auto pixel_offset = 0;
        auto vector_index = 0;
        while(num_to_read) {
            auto size = std::min(pixel_stride, num_to_read);
            integer_column_reader->read(buffer, column_chunk_encoding, pixel_offset, size, pixel_stride, vector_index,
                int_result_vector,
                *integer_column_writer->getColumnChunkIndexPtr(),
                bit_mask);
            for(int i = vector_index; i < vector_index + size; i++) {
                std::cerr << "[DEBUG READ CASE1] " << int_result_vector->intVector[i] << std::endl;
                // EXPECT_EQ(int_result_vector->intVector[i], integer_column_vector->intVector[i]);
            }
            pixel_offset+=size;
            vector_index+=size;
            num_to_read-=size;
        }
}