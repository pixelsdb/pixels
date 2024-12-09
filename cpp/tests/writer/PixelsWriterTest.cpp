#include "PixelsWriterImpl.h"

#include "gtest/gtest.h"

class PIXELS_WRITER_TEST : public ::testing::Test
{
protected:
protected:
    const int pixels_stride_ = 16;
    const std::string target_file_path_{"test.dat"};
    const int block_size_ = 1024;
    const int compression_block_size_ = 16;
    bool block_padding_ = true;
    int row_num = 8888;
    const int row_group_size_ = 10;
};

TEST_F(PIXELS_WRITER_TEST, SINGLE_INT)
{
    auto schema = TypeDescription::fromString("struct<a:int>");
    EXPECT_TRUE(schema);
    std::vector<bool> encode_vector(1, true);
    auto row_batch = schema->createRowBatch(row_group_size_, encode_vector);

    EncodingLevel encoding_level{EncodingLevel::EL2};
    bool nulls_padding = true;
    bool partitioned = true;

    auto pixels_writer = std::make_unique<PixelsWriterImpl>(schema, pixels_stride_, row_group_size_, target_file_path_,
                                                            block_size_, block_padding_, encoding_level, nulls_padding, partitioned, compression_block_size_);

    /**=======================
     * *       INFO
     *  Write Row Batch
     *
     *========================**/
    {
        auto va = std::dynamic_pointer_cast<LongColumnVector>(row_batch->cols[0]);
        ASSERT_TRUE(va);
        auto start_time_ts = std::chrono::high_resolution_clock::now();
        {
            for (int i = 0; i < row_num; ++i)
            {
                auto row = row_batch->rowCount++;
                va->add(i);
                if (row_batch->rowCount == row_batch->getMaxSize())
                {
                    pixels_writer->addRowBatch(row_batch);
                    row_batch->reset();
                } 
            }
            if(row_batch->rowCount!=0) {
                pixels_writer->addRowBatch(row_batch);
                row_batch->reset();
            }
            pixels_writer->close();
        }
        auto end_time_ts = std::chrono::high_resolution_clock::now();
        auto duration = end_time_ts - start_time_ts;
        std::cerr << "[DEBUG] Time: " << duration.count() << std::endl;
    }
}