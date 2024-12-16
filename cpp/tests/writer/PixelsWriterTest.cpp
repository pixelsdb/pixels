#include "PixelsWriterImpl.h"
#include "PixelsReaderImpl.h"
#include "physical/PhysicalReaderUtil.h"
#include "gtest/gtest.h"

class PIXELS_WRITER_TEST : public ::testing::Test
{
protected:
protected:
    const int pixels_stride_ = 16;
    const std::string target_file_path_{"/home/anti/work/anti_pixels/cpp/build/debug/test.pxl"};
    const int block_size_ = 1024;
    const int compression_block_size_ = 16;
    bool block_padding_ = true;
    int row_num = 10;
    const int row_group_size_ = 10;
};

TEST_F(PIXELS_WRITER_TEST, DISABLED_SINGLE_INT)
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

TEST_F(PIXELS_WRITER_TEST, WRITE_AND_READ)
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

    {
        auto storage = Storage::fromPath("file:/" + target_file_path_);
        auto physical_reader = PhysicalReaderUtil::newPhysicalReader(storage, target_file_path_);

        auto footer_cache = std::make_shared<PixelsFooterCache>();
        auto file_tail = std::make_shared<pixels::proto::FileTail>();
        {
            // get file tail
                long fileLen = physical_reader->getFileLength();
                physical_reader->seek(fileLen - sizeof(long));
                long fileTailOffset = physical_reader->readLong();
                int fileTailLength = (int) (fileLen - fileTailOffset - sizeof(long));
                physical_reader->seek(fileTailOffset);
                auto fileTailBuffer = physical_reader->readFully(fileTailLength);
                file_tail->ParseFromArray(fileTailBuffer->getPointer(), fileTailBuffer->size());
                footer_cache->putFileTail(target_file_path_, file_tail);
        }
        // test read 
        auto pixels_reader = std::make_unique<PixelsReaderImpl>(
            schema, 
            physical_reader,
            file_tail,
            footer_cache
        );
        std::cerr<< "[DEBUG] row group num:" << pixels_reader->getRowGroupNum() << std::endl;

        PixelsReaderOption option;
        option.setSkipCorruptRecords(false);
        option.setTolerantSchemaEvolution(true);
        option.setEnableEncodedColumnVector(true);
        option.setIncludeCols({"a"});
        option.setBatchSize(10);
        option.setRGRange(0,1);
        auto recordReader = pixels_reader->read(option);
        auto rowBatch = recordReader->readBatch(true);
        auto vector = std::static_pointer_cast<LongColumnVector>(rowBatch->cols[0]);
        {
            // check read result
            for(int i = 0; i < row_num; i++) {
                EXPECT_EQ(vector->intVector[i], i);
            }
        }
        auto pause = true;
        return;
    }

}