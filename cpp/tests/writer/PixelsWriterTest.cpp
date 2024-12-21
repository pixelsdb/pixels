/*
 * Copyright 2024 PixelsDB.
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
 * @author whz
 * @create 2024-11-25
 */
#include "PixelsWriterImpl.h"
#include "PixelsReaderImpl.h"
#include "physical/PhysicalReaderUtil.h"
#include "PixelsReaderBuilder.h"
#include "gtest/gtest.h"

class PIXELS_WRITER_TEST : public ::testing::Test
{
protected:
    void SetUp() override {
        target_file_path_ =  ConfigFactory::Instance().getPixelsSourceDirectory() + 
            "cpp/tests/data/example.pxl";
    }
protected:
    const int pixels_stride_ = 16;
    std::string target_file_path_;
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

TEST_F(PIXELS_WRITER_TEST, DISABLED_WRITE_AND_READ)
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
        auto footerCache = std::make_shared<PixelsFooterCache>();
	    auto builder = std::make_shared<PixelsReaderBuilder>();
        // test read 
	    std::shared_ptr<::Storage> storage = StorageFactory::getInstance()->getStorage(::Storage::file);
	    std::shared_ptr<PixelsReader> pixels_reader = builder
	                                 ->setPath(target_file_path_)
	                                 ->setStorage(storage)
	                                 ->setPixelsFooterCache(footerCache)
	                                 ->build();
        
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
                EXPECT_EQ(*( (int*)vector->intVector + i ), i);
            }
        }
        auto pause = true;
        return;
    }

}

TEST_F(PIXELS_WRITER_TEST, DISABLED_WRITE_TWO_COLUMN)
{
    auto schema = TypeDescription::fromString("struct<a:int, b:int>");
    EXPECT_TRUE(schema);
    std::vector<bool> encode_vector(2, true);
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
        auto vb = std::dynamic_pointer_cast<LongColumnVector>(row_batch->cols[1]);
        ASSERT_TRUE(va);
        auto start_time_ts = std::chrono::high_resolution_clock::now();
        {
            for (int i = 0; i < row_num; ++i)
            {
                auto row = row_batch->rowCount++;
                va->add(i);
                vb->add(i * i);
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

TEST_F(PIXELS_WRITER_TEST, DISABLED_SINGLE_INT_WITHOUT_RUNLENENCODE)
{
    auto schema = TypeDescription::fromString("struct<a:int>");
    EXPECT_TRUE(schema);
    std::vector<bool> encode_vector(1, true);
    auto row_batch = schema->createRowBatch(row_group_size_, encode_vector);

    EncodingLevel encoding_level{EncodingLevel::EL0};
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

TEST_F(PIXELS_WRITER_TEST, WRITE_TWO_COLUMN_WITHOUT_RUNLENENCODE)
{
    auto schema = TypeDescription::fromString("struct<a:int, b:int>");
    EXPECT_TRUE(schema);
    std::vector<bool> encode_vector(2, true);
    auto row_batch = schema->createRowBatch(row_group_size_, encode_vector);

    EncodingLevel encoding_level{EncodingLevel::EL0};
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
        auto vb = std::dynamic_pointer_cast<LongColumnVector>(row_batch->cols[1]);
        ASSERT_TRUE(va);
        auto start_time_ts = std::chrono::high_resolution_clock::now();
        {
            for (int i = 0; i < row_num; ++i)
            {
                auto row = row_batch->rowCount++;
                va->add(i);
                vb->add(i * i);
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