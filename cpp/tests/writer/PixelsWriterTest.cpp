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
#include <filesystem>
#include <chrono>


class PIXELS_WRITER_TEST : public ::testing::Test
{
protected:
    void SetUp() override {
        base_path = ConfigFactory::Instance().getPixelsSourceDirectory() + "cpp/tests/data/";
        if(!std::filesystem::exists(base_path)) {
            std::filesystem::create_directories(base_path);
        }
        target_file_path_ = base_path + "example.pxl";
        
        // if exist, delete it first
        if (std::filesystem::exists(target_file_path_)) {
            std::filesystem::remove(target_file_path_);
            std::cout << "[INFO] Removed existing test file: " << target_file_path_ << std::endl;
        }
    }

    void cleanup(const std::string& path) {
        if (std::filesystem::exists(path)) {
            std::filesystem::remove(path);
        }
    }

protected:
    std::string base_path;
    // `pixels_stride` must be divisible by `row_batch`
    const int pixels_stride_ = 20;
    std::string target_file_path_;
    const int block_size_ = 1024;
    const int compression_block_size_ = 16;
    bool block_padding_ = true;
    int row_num = 10;
    const int row_group_size_ = 100;
};

TEST_F(PIXELS_WRITER_TEST, DISABLED_SINGLE_INT)
{
    auto schema = TypeDescription::fromString("struct<a:int>");
    EXPECT_TRUE(schema);
    std::vector<bool> encode_vector(1, true);
    auto row_batch = schema->createRowBatch(row_num, encode_vector);

    EncodingLevel encoding_level{EncodingLevel::EL0};
    bool nulls_padding = true;
    bool partitioned = true;

    auto pixels_writer = std::make_unique<PixelsWriterImpl>(schema, pixels_stride_, row_group_size_, target_file_path_,
                                                            block_size_, block_padding_, encoding_level, nulls_padding, partitioned, compression_block_size_);

    auto va = std::dynamic_pointer_cast<IntColumnVector>(row_batch->cols[0]);
    ASSERT_TRUE(va);
    for (int i = 0; i < row_num; ++i)
    {
        va->add(i);
        row_batch->rowCount++;
        if (row_batch->rowCount == row_batch->getMaxSize())
        {
            pixels_writer->addRowBatch(row_batch);
            row_batch->reset();
        } 
    }
    if(row_batch->rowCount != 0) {
        pixels_writer->addRowBatch(row_batch);
        row_batch->reset();
    }
    pixels_writer->close();

    // Read back and verify
    auto footerCache = std::make_shared<PixelsFooterCache>();
    auto builder = std::make_shared<PixelsReaderBuilder>();
    std::shared_ptr<::Storage> storage = StorageFactory::getInstance()->getStorage(::Storage::file);
    std::shared_ptr<PixelsReader> pixels_reader = builder
                                    ->setPath(target_file_path_)
                                    ->setStorage(storage)
                                    ->setPixelsFooterCache(footerCache)
                                    ->build();
    
    PixelsReaderOption option;
    option.setSkipCorruptRecords(false);
    option.setTolerantSchemaEvolution(true);
    option.setEnableEncodedColumnVector(true);
    option.setIncludeCols({"a"});
    option.setBatchSize(10);
    option.setRGRange(0,1);
    auto recordReader = pixels_reader->read(option);
    int count = 0;
    while(true) {
        auto rb = recordReader->readBatch(true);
        if(rb == nullptr || rb->rowCount == 0) break;
        auto v = std::static_pointer_cast<IntColumnVector>(rb->cols[0]);
        for(int i = 0; i < rb->rowCount; i++) {
            EXPECT_EQ(((int*)v->intVector)[i], count);
            count++;
        }
    }
    EXPECT_EQ(count, row_num);
    ::BufferPool::Reset();

}

TEST_F(PIXELS_WRITER_TEST, SINGLE_LONG)
{
    auto schema = TypeDescription::fromString("struct<a:long>");
    EXPECT_TRUE(schema);
    std::vector<bool> encode_vector(1, true);
    auto row_batch = schema->createRowBatch(row_num, encode_vector);
    
    EncodingLevel encoding_level{EncodingLevel::EL0};
    bool nulls_padding = true;
    bool partitioned = true;
    
    auto pixels_writer = std::make_unique<PixelsWriterImpl>(schema, pixels_stride_, row_group_size_, target_file_path_,
                                                            block_size_, block_padding_, encoding_level, nulls_padding, partitioned, compression_block_size_);

    auto va = std::dynamic_pointer_cast<LongColumnVector>(row_batch->cols[0]);
    ASSERT_TRUE(va);
    for (long i = 0; i < row_num; ++i)
    {
        va->add(i*1000000L);
        // va->add(9110818468285196899L);
        row_batch->rowCount++;
        if (row_batch->rowCount == row_batch->getMaxSize())
        {
            pixels_writer->addRowBatch(row_batch);
            row_batch->reset();
        } 
    }
    if(row_batch->rowCount != 0) {
        pixels_writer->addRowBatch(row_batch);
        row_batch->reset();
    }
    pixels_writer->close();

    // Read back and verify
    auto footerCache = std::make_shared<PixelsFooterCache>();
    auto builder = std::make_shared<PixelsReaderBuilder>();
    std::shared_ptr<::Storage> storage = StorageFactory::getInstance()->getStorage(::Storage::file);
    std::shared_ptr<PixelsReader> pixels_reader = builder
                                    ->setPath(target_file_path_)
                                    ->setStorage(storage)
                                    ->setPixelsFooterCache(footerCache)
                                    ->build();
    
    PixelsReaderOption option;
    option.setSkipCorruptRecords(false);
    option.setTolerantSchemaEvolution(true);
    option.setEnableEncodedColumnVector(true);
    option.setIncludeCols({"a"});
    option.setBatchSize(10);
    option.setRGRange(0,1);
    auto recordReader = pixels_reader->read(option);
    int count = 0;
    while(true) {
        auto rb = recordReader->readBatch(true);
        if(rb == nullptr || rb->rowCount == 0) break;
        auto v = std::static_pointer_cast<LongColumnVector>(rb->cols[0]);
        for(int i = 0; i < rb->rowCount; i++) {
            EXPECT_EQ(v->longVector[i], (long)count * 1000000L);
            count++;
        }
    }
    EXPECT_EQ(count, row_num);
    ::BufferPool::Reset();

}

TEST_F(PIXELS_WRITER_TEST, DISABLED_SINGLE_STRING)
{
    auto schema = TypeDescription::fromString("struct<a:string>");
    EXPECT_TRUE(schema);
    std::vector<bool> encode_vector(1, true);
    auto row_batch = schema->createRowBatch(row_num, encode_vector);
    
    EncodingLevel encoding_level{EncodingLevel::EL2};
    bool nulls_padding = true;
    bool partitioned = true;
    
    auto pixels_writer = std::make_unique<PixelsWriterImpl>(schema, pixels_stride_, row_group_size_, target_file_path_,
                                                            block_size_, block_padding_, encoding_level, nulls_padding, partitioned, compression_block_size_);

    auto va = std::dynamic_pointer_cast<BinaryColumnVector>(row_batch->cols[0]);
    ASSERT_TRUE(va);
    for (int i = 0; i < row_num; ++i)
    {
        std::string s = "string_" + std::to_string(i);
        va->add(s);
        row_batch->rowCount++;
        if (row_batch->rowCount == row_batch->getMaxSize())
        {
            pixels_writer->addRowBatch(row_batch);
            row_batch->reset();
        } 
    }
    if(row_batch->rowCount != 0) {
        pixels_writer->addRowBatch(row_batch);
        row_batch->reset();
    }
    pixels_writer->close();

    // Read back and verify
    auto footerCache = std::make_shared<PixelsFooterCache>();
    auto builder = std::make_shared<PixelsReaderBuilder>();
    std::shared_ptr<::Storage> storage = StorageFactory::getInstance()->getStorage(::Storage::file);
    std::shared_ptr<PixelsReader> pixels_reader = builder
                                    ->setPath(target_file_path_)
                                    ->setStorage(storage)
                                    ->setPixelsFooterCache(footerCache)
                                    ->build();
    
    PixelsReaderOption option;
    option.setSkipCorruptRecords(false);
    option.setTolerantSchemaEvolution(true);
    option.setEnableEncodedColumnVector(true);
    option.setIncludeCols({"a"});
    option.setBatchSize(10);
    option.setRGRange(0,1);
    auto recordReader = pixels_reader->read(option);
    int count = 0;
    while(true) {
        auto rb = recordReader->readBatch(true);
        if(rb == nullptr || rb->rowCount == 0) break;
        auto v = std::static_pointer_cast<BinaryColumnVector>(rb->cols[0]);
        for(int i = 0; i < rb->rowCount; i++) {
            std::string expected = "string_" + std::to_string(count);
            std::string actual(v->vector[i].GetString());
            EXPECT_EQ(actual, expected);
            count++;
        }
    }
    EXPECT_EQ(count, row_num);
    ::BufferPool::Reset();

}

TEST_F(PIXELS_WRITER_TEST, DISABLED_MULTI_COLUMN)
{
    auto schema = TypeDescription::fromString("struct<a:int,b:long,c:string>");
    EXPECT_TRUE(schema);
    std::vector<bool> encode_vector(3, true);
    auto row_batch = schema->createRowBatch(row_num, encode_vector);
    EncodingLevel encoding_level{EncodingLevel::EL2};
    auto pixels_writer = std::make_unique<PixelsWriterImpl>(schema, pixels_stride_, row_group_size_, target_file_path_,
                                                            block_size_, block_padding_, encoding_level, true, true, compression_block_size_);

    auto va = std::dynamic_pointer_cast<IntColumnVector>(row_batch->cols[0]);
    auto vb = std::dynamic_pointer_cast<LongColumnVector>(row_batch->cols[1]);
    auto vc = std::dynamic_pointer_cast<BinaryColumnVector>(row_batch->cols[2]);
    
    for (int i = 0; i < row_num; ++i)
    {
        va->add(i);
        vb->add((long)i * 10);
        std::string s = "val_" + std::to_string(i);
        vc->add(s);
        row_batch->rowCount++;
        if (row_batch->rowCount == row_batch->getMaxSize())
        {
            pixels_writer->addRowBatch(row_batch);
            row_batch->reset();
        } 
    }
    if(row_batch->rowCount != 0) {
        pixels_writer->addRowBatch(row_batch);
        row_batch->reset();
    }
    pixels_writer->close();

    // Read back and verify
    auto footerCache = std::make_shared<PixelsFooterCache>();
    auto builder = std::make_shared<PixelsReaderBuilder>();
    std::shared_ptr<::Storage> storage = StorageFactory::getInstance()->getStorage(::Storage::file);
    std::shared_ptr<PixelsReader> pixels_reader = builder->setPath(target_file_path_)->setStorage(storage)->setPixelsFooterCache(footerCache)->build();
    
    PixelsReaderOption option;
    option.setSkipCorruptRecords(false);
    option.setTolerantSchemaEvolution(true);
    option.setEnableEncodedColumnVector(true);
    option.setBatchSize(10);
    option.setRGRange(0,1);
    option.setIncludeCols({"a", "b", "c"});
    auto recordReader = pixels_reader->read(option);
    int count = 0;
    while(true) {
        auto rb = recordReader->readBatch(true);
        if(rb == nullptr || rb->rowCount == 0) break;
        auto ra = std::static_pointer_cast<IntColumnVector>(rb->cols[0]);
        auto rb_col = std::static_pointer_cast<LongColumnVector>(rb->cols[1]);
        auto rc = std::static_pointer_cast<BinaryColumnVector>(rb->cols[2]);
        for(int i = 0; i < rb->rowCount; i++) {
            EXPECT_EQ(((int*)ra->intVector)[i], count);
            EXPECT_EQ(rb_col->longVector[i], (long)count * 10);
            std::string expected = "val_" + std::to_string(count);
            std::string actual(rc->vector[i].GetString());
            EXPECT_EQ(actual, expected);
            count++;
        }
    }
    EXPECT_EQ(count, row_num);
    ::BufferPool::Reset();

}

TEST_F(PIXELS_WRITER_TEST, DISABLED_MULTI_ROWGROUP)
{
    // Write 100 rows with row_group_size = 10 -> should result in 10 row groups
    int total_rows = 100;
    auto schema = TypeDescription::fromString("struct<a:int>");
    EXPECT_TRUE(schema);
    std::vector<bool> encode_vector(1, true);
    auto row_batch = schema->createRowBatch(row_num, encode_vector);
    EncodingLevel encoding_level{EncodingLevel::EL0};
    auto pixels_writer = std::make_unique<PixelsWriterImpl>(schema, pixels_stride_, row_group_size_, target_file_path_,
                                                            block_size_, block_padding_, encoding_level, true, true, compression_block_size_);

    auto va = std::dynamic_pointer_cast<IntColumnVector>(row_batch->cols[0]);
    for (int i = 0; i < total_rows; ++i)
    {
        va->add(i);
        row_batch->rowCount++;
        if (row_batch->rowCount == row_batch->getMaxSize())
        {
            pixels_writer->addRowBatch(row_batch);
            row_batch->reset();
        } 
    }
    if(row_batch->rowCount != 0) {
        pixels_writer->addRowBatch(row_batch);
        row_batch->reset();
    }
    pixels_writer->close();

    // Read back and verify row group count and data
    auto footerCache = std::make_shared<PixelsFooterCache>();
    auto builder = std::make_shared<PixelsReaderBuilder>();
    std::shared_ptr<::Storage> storage = StorageFactory::getInstance()->getStorage(::Storage::file);
    std::shared_ptr<PixelsReader> pixels_reader = builder->setPath(target_file_path_)->setStorage(storage)->setPixelsFooterCache(footerCache)->build();
    
    EXPECT_GE(pixels_reader->getRowGroupNum(), 3);

    PixelsReaderOption option;
    option.setSkipCorruptRecords(false);
    option.setTolerantSchemaEvolution(true);
    option.setEnableEncodedColumnVector(true);
    option.setBatchSize(10);
    option.setRGRange(0,3);
    option.setIncludeCols({"a"});
    auto recordReader = pixels_reader->read(option);
    int count = 0;
    while(true) {
        auto rb = recordReader->readBatch(true);
        if(rb == nullptr || rb->rowCount == 0) break;
        auto v = std::static_pointer_cast<IntColumnVector>(rb->cols[0]);
        for(int i = 0; i < rb->rowCount; i++) {
            EXPECT_EQ(((int*)v->intVector)[i], count);
            count++;
        }
    }
    EXPECT_EQ(count, total_rows);
    ::BufferPool::Reset();

}

TEST_F(PIXELS_WRITER_TEST, DISABLED_MULTI_FILE)
{
    auto schema = TypeDescription::fromString("struct<a:int>");
    EXPECT_TRUE(schema);
    std::vector<bool> encode_vector(1, true);
    
    std::string file1 = base_path + "example_1.pxl";
    std::string file2 = base_path + "example_2.pxl";

    auto write_file = [&](const std::string& path, int start_val) {
        auto row_batch = schema->createRowBatch(row_num, encode_vector);
        EncodingLevel encoding_level{EncodingLevel::EL2};
        auto pixels_writer = std::make_unique<PixelsWriterImpl>(schema, pixels_stride_, row_group_size_, path,
                                                                block_size_, block_padding_, encoding_level, true, true, compression_block_size_);
        auto va = std::dynamic_pointer_cast<IntColumnVector>(row_batch->cols[0]);
        for (int i = 0; i < row_num; ++i) {
            va->add(start_val + i);
            row_batch->rowCount++;
            if (row_batch->rowCount == row_batch->getMaxSize()) {
                pixels_writer->addRowBatch(row_batch);
                row_batch->reset();
            }
        }
        if(row_batch->rowCount != 0) {
            pixels_writer->addRowBatch(row_batch);
        }
        pixels_writer->close();
    };

    write_file(file1, 0);
    write_file(file2, 100);

    auto verify_file = [&](const std::string& path, int expected_start) {
        auto footerCache = std::make_shared<PixelsFooterCache>();
        auto builder = std::make_shared<PixelsReaderBuilder>();
        std::shared_ptr<::Storage> storage = StorageFactory::getInstance()->getStorage(::Storage::file);
        std::shared_ptr<PixelsReader> pixels_reader = builder->setPath(path)->setStorage(storage)->setPixelsFooterCache(footerCache)->build();
        
        PixelsReaderOption option;
        option.setSkipCorruptRecords(false);
        option.setTolerantSchemaEvolution(true);
        option.setEnableEncodedColumnVector(true);
        option.setBatchSize(10);
        option.setRGRange(0,1);
        option.setIncludeCols({"a"});
        auto recordReader = pixels_reader->read(option);
        int count = 0;
        while(true) {
            auto rb = recordReader->readBatch(true);
            if(rb == nullptr || rb->rowCount == 0) break;
            auto v = std::static_pointer_cast<IntColumnVector>(rb->cols[0]);
            for(int i = 0; i < rb->rowCount; i++) {
                EXPECT_EQ(((int*)v->intVector)[i], expected_start + count);
                count++;
            }
        }
        EXPECT_EQ(count, row_num);
    };

    verify_file(file1, 0);
    verify_file(file2, 100);

    cleanup(file1);
    cleanup(file2);
    ::BufferPool::Reset();

}

TEST_F(PIXELS_WRITER_TEST, DISABLED_COMPREHENSIVE_TEST)
{
    // 测试多种数据类型写入多个rowgroup 一次测试写入多个文件
    int total_rows = 50;
    int rg_size = 10;
    auto schema = TypeDescription::fromString("struct<c1:int,c2:long,c3:string,c4:date,c5:timestamp,c6:decimal(10,2)>");
    EXPECT_TRUE(schema);
    std::vector<bool> encode_vector(6, true);
    
    std::string file1 = base_path + "example_comp1.pxl";
    std::string file2 = base_path+ "example_comp2.pxl";


    auto write_comp_file = [&](const std::string& path, int start_val) {
        auto row_batch = schema->createRowBatch(rg_size, encode_vector);
        EncodingLevel encoding_level{EncodingLevel::EL2};
        auto pixels_writer = std::make_unique<PixelsWriterImpl>(schema, pixels_stride_, rg_size, path,
                                                                block_size_, block_padding_, encoding_level, true, true, compression_block_size_);
        
        auto v1 = std::dynamic_pointer_cast<IntColumnVector>(row_batch->cols[0]);
        auto v2 = std::dynamic_pointer_cast<LongColumnVector>(row_batch->cols[1]);
        auto v3 = std::dynamic_pointer_cast<BinaryColumnVector>(row_batch->cols[2]);
        auto v4 = std::dynamic_pointer_cast<DateColumnVector>(row_batch->cols[3]);
        auto v5 = std::dynamic_pointer_cast<TimestampColumnVector>(row_batch->cols[4]);
        auto v6 = std::dynamic_pointer_cast<DecimalColumnVector>(row_batch->cols[5]);

        for (int i = 0; i < total_rows; ++i) {
            int val = start_val + i;
            v1->add(val);
            v2->add((long)val * 100);
            std::string tmp = "str_" + std::to_string(val);
            v3->add(tmp);
            v4->add(val); // days from epoch
            v5->add((long)val * 1000); // timestamp
            v6->add((long)val); // decimal value

            row_batch->rowCount++;
            if (row_batch->rowCount == row_batch->getMaxSize()) {
                pixels_writer->addRowBatch(row_batch);
                row_batch->reset();
            }
        }
        if(row_batch->rowCount != 0) {
            pixels_writer->addRowBatch(row_batch);
        }
        pixels_writer->close();
    };

    write_comp_file(file1, 0);
    write_comp_file(file2, 1000);

    auto verify_comp_file = [&](const std::string& path, int expected_start) {
        auto footerCache = std::make_shared<PixelsFooterCache>();
        auto builder = std::make_shared<PixelsReaderBuilder>();
        std::shared_ptr<::Storage> storage = StorageFactory::getInstance()->getStorage(::Storage::file);
        std::shared_ptr<PixelsReader> pixels_reader = builder->setPath(path)->setStorage(storage)->setPixelsFooterCache(footerCache)->build();
        
        EXPECT_EQ(pixels_reader->getRowGroupNum(), total_rows / rg_size);

        PixelsReaderOption option;
        option.setSkipCorruptRecords(false);
        option.setTolerantSchemaEvolution(true);
        option.setEnableEncodedColumnVector(true);
        option.setBatchSize(10);
        option.setRGRange(0,5);
        option.setIncludeCols({"c1", "c2", "c3", "c4", "c5", "c6"});
        auto recordReader = pixels_reader->read(option);
        int count = 0;
        while(true) {
            auto rb = recordReader->readBatch(true);
            if(rb == nullptr || rb->rowCount == 0) break;
            
            auto rv1 = std::static_pointer_cast<IntColumnVector>(rb->cols[0]);
            auto rv2 = std::static_pointer_cast<LongColumnVector>(rb->cols[1]);
            auto rv3 = std::static_pointer_cast<BinaryColumnVector>(rb->cols[2]);
            auto rv4 = std::static_pointer_cast<DateColumnVector>(rb->cols[3]);
            auto rv5 = std::static_pointer_cast<TimestampColumnVector>(rb->cols[4]);
            auto rv6 = std::static_pointer_cast<DecimalColumnVector>(rb->cols[5]);

            for(int i = 0; i < rb->rowCount; i++) {
                int expected_val = expected_start + count;
                EXPECT_EQ(((int*)rv1->intVector)[i], expected_val);
                EXPECT_EQ(rv2->longVector[i], (long)expected_val * 100);
                
                std::string expected_str = "str_" + std::to_string(expected_val);
                std::string actual_str(rv3->vector[i].GetString());
                EXPECT_EQ(actual_str, expected_str);
                
                EXPECT_EQ(rv4->dates[i], expected_val);
                EXPECT_EQ(rv5->times[i], (long)expected_val * 1000);
                EXPECT_EQ(rv6->vector[i], (long)expected_val);
                
                count++;
            }
        }
        EXPECT_EQ(count, total_rows);
    };

    verify_comp_file(file1, 0);
    verify_comp_file(file2, 1000);

    cleanup(file1);
    cleanup(file2);
    ::BufferPool::Reset();

}
