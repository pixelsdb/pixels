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
#include "vector/IntColumnVector.h"
#include "vector/LongColumnVector.h"
#include "reader/IntColumnReader.h"
#include "reader/LongColumnReader.h"
#include "writer/IntColumnWriter.h"
#include "writer/LongColumnWriter.h"
#include "pixels_generated.h"


#include "gtest/gtest.h"
#include <array>

TEST(IntWriterTest, WriteRunLengthEncodeIntWithoutNull) {
  int len = 10;
  int pixel_stride = 5;
  bool encoding = true;
  auto integer_column_vector =
      std::make_shared<IntColumnVector>(len, encoding);
  ASSERT_TRUE(integer_column_vector);
  for (int i = 0; i < len; ++i) {
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

  flatbuffers::FlatBufferBuilder fbb(1024);

  auto integer_column_writer = std::make_unique<IntColumnWriter>(
      TypeDescription::createInt(), option);
  auto write_size = integer_column_writer->write( integer_column_vector, len);
  EXPECT_NE(write_size, 0);
  integer_column_writer->flush();
  auto content = integer_column_writer->getColumnChunkContent();
  EXPECT_GT(content.size(), 0);

  std::cerr << "[DEBUG] content size: " << content.size() << std::endl;

  /**----------------------
   **      Write End. Use Reader to check
   *------------------------**/
  auto integer_column_reader =
      std::make_unique<IntColumnReader>(TypeDescription::createInt());
  auto buffer = std::make_shared<ByteBuffer>(content.size());
  buffer->putBytes(content.data(), content.size());
  
  auto column_chunk_encoding_offset = integer_column_writer->getColumnChunkEncoding(fbb);
  auto column_chunk_index_offset = integer_column_writer->buildColumnChunkIndex(fbb, 0, content.size(), true);

  std::vector<flatbuffers::Offset<pixels::fb::ColumnChunkIndex>> indexVec;
  indexVec.push_back(column_chunk_index_offset);
  auto indicesOffset = fbb.CreateVector(indexVec);
  auto rowGroupIndexOffset = pixels::fb::CreateRowGroupIndex(fbb, indicesOffset);

  std::vector<flatbuffers::Offset<pixels::fb::ColumnEncoding>> encodingVec;
  encodingVec.push_back(column_chunk_encoding_offset);
  auto encodingsOffset = fbb.CreateVector(encodingVec);
  auto rowGroupEncodingOffset = pixels::fb::CreateRowGroupEncoding(fbb, encodingsOffset);

  auto footerOffset = pixels::fb::CreateRowGroupFooter(fbb, rowGroupIndexOffset, rowGroupEncodingOffset);
  fbb.Finish(footerOffset);

  auto footer = flatbuffers::GetRoot<pixels::fb::RowGroupFooter>((fbb.GetBufferPointer()));
  auto column_chunk_encoding = footer->rowGroupEncoding()->columnChunkEncodings()->Get(0);
  auto column_chunk_index = footer->rowGroupIndexEntry()->columnChunkIndexEntries()->Get(0);

  auto int_result_vector =
      std::make_shared<IntColumnVector>(len, encoding);
  auto bit_mask = std::make_shared<PixelsBitMask>(len);

  auto num_to_read = len;
  auto pixel_offset = 0;
  auto vector_index = 0;
  while (num_to_read) {
    auto size = std::min(pixel_stride, num_to_read);
    integer_column_reader->read(
        buffer, column_chunk_encoding, pixel_offset, size, pixel_stride,
        vector_index, int_result_vector,
        column_chunk_index, bit_mask);
    for (int i = vector_index; i < vector_index + size; i++) {
      std::cerr << "[DEBUG READ CASE1] " << int_result_vector->intVector[i]
                << std::endl;
      EXPECT_EQ(int_result_vector->intVector[i],
                integer_column_vector->intVector[i]);
    }
    pixel_offset += size;
    vector_index += size;
    num_to_read -= size;
  }
  integer_column_writer->close();
}

TEST(IntWriterTest, WriteIntWithoutNull) {
  int len = 10;
  int pixel_stride = 5;
  bool encoding = false;
  auto integer_column_vector =
      std::make_shared<IntColumnVector>(len, encoding);
  ASSERT_TRUE(integer_column_vector);
  for (int i = 0; i < len; ++i) {
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

  flatbuffers::FlatBufferBuilder fbb(1024);

  auto integer_column_writer = std::make_unique<IntColumnWriter>(
      TypeDescription::createInt(), option);
  auto write_size = integer_column_writer->write(integer_column_vector, len);
  EXPECT_NE(write_size, 0);
  integer_column_writer->flush();
  auto content = integer_column_writer->getColumnChunkContent();
  EXPECT_GT(content.size(), 0);

  std::cerr << "[DEBUG] content size: " << content.size() << std::endl;

  /**----------------------
   **      Write End. Use Reader to check
   *------------------------**/
  auto integer_column_reader =
      std::make_unique<IntColumnReader>(TypeDescription::createInt());
  auto buffer = std::make_shared<ByteBuffer>(content.size());
  buffer->putBytes(content.data(), content.size());
  
  auto column_chunk_encoding_offset = integer_column_writer->getColumnChunkEncoding(fbb);
  auto column_chunk_index_offset = integer_column_writer->buildColumnChunkIndex(fbb, 0, content.size(), true);

  std::vector<flatbuffers::Offset<pixels::fb::ColumnChunkIndex>> indexVec;
  indexVec.push_back(column_chunk_index_offset);
  auto indicesOffset = fbb.CreateVector(indexVec);
  auto rowGroupIndexOffset = pixels::fb::CreateRowGroupIndex(fbb, indicesOffset);

  std::vector<flatbuffers::Offset<pixels::fb::ColumnEncoding>> encodingVec;
  encodingVec.push_back(column_chunk_encoding_offset);
  auto encodingsOffset = fbb.CreateVector(encodingVec);
  auto rowGroupEncodingOffset = pixels::fb::CreateRowGroupEncoding(fbb, encodingsOffset);

  auto footerOffset = pixels::fb::CreateRowGroupFooter(fbb, rowGroupIndexOffset, rowGroupEncodingOffset);
  fbb.Finish(footerOffset);

  auto footer = flatbuffers::GetRoot<pixels::fb::RowGroupFooter>((fbb.GetBufferPointer()));
  auto column_chunk_encoding = footer->rowGroupEncoding()->columnChunkEncodings()->Get(0);
  auto column_chunk_index = footer->rowGroupIndexEntry()->columnChunkIndexEntries()->Get(0);

  auto int_result_vector =
      std::make_shared<IntColumnVector>(len, encoding);
  auto bit_mask = std::make_shared<PixelsBitMask>(len);

  auto num_to_read = len;
  auto pixel_offset = 0;
  auto vector_index = 0;
  while (num_to_read) {
    auto size = std::min(pixel_stride, num_to_read);
    integer_column_reader->read(
        buffer, column_chunk_encoding, pixel_offset, size, pixel_stride,
        vector_index, int_result_vector,
        column_chunk_index, bit_mask);
    for (int i = vector_index; i < vector_index + size; i++) {
      std::cerr << "[DEBUG READ CASE1] "
                << int_result_vector->intVector[i]
                << std::endl;
      EXPECT_EQ(int_result_vector->intVector[i],
                integer_column_vector->intVector[i]);
    }
    pixel_offset += size;
    vector_index += size;
    num_to_read -= size;
  }
  integer_column_writer->close();
}

TEST(IntWriterTest, WriteRunLengthEncodeLongWithoutNull) {
  int len = 23;
  int pixel_stride = 5;
  bool encoding = true;
  bool is_long = true;
  auto long_column_vector =
      std::make_shared<LongColumnVector>(len, encoding, is_long);
  ASSERT_TRUE(long_column_vector);
  for (long i = 0; i < len; ++i) {
    long_column_vector->add(INT64_MAX - i);
  }
  // Run Length Encoding
  auto option = std::make_shared<PixelsWriterOption>();
  option->setPixelsStride(pixel_stride);
  option->setNullsPadding(false);
  option->setEncodingLevel(EncodingLevel(EncodingLevel::EL2));

  flatbuffers::FlatBufferBuilder fbb(1024);

  auto long_column_writer = std::make_unique<LongColumnWriter>(
      TypeDescription::createLong(), option);
  auto write_size = long_column_writer->write(long_column_vector, len);
  EXPECT_NE(write_size, 0);
  long_column_writer->flush();
  auto content = long_column_writer->getColumnChunkContent();
  EXPECT_GT(content.size(), 0);
  std::cerr << "[DEBUG] content size: " << content.size() << std::endl;

  /**----------------------
   **      Write End. Use Reader to check
   *------------------------**/
  auto long_column_reader =
      std::make_unique<LongColumnReader>(TypeDescription::createLong());
  auto buffer = std::make_shared<ByteBuffer>(content.size());
  buffer->putBytes(content.data(), content.size());
  
  auto column_chunk_encoding_offset = long_column_writer->getColumnChunkEncoding(fbb);
  auto column_chunk_index_offset = long_column_writer->buildColumnChunkIndex(fbb, 0, content.size(), true);

  std::vector<flatbuffers::Offset<pixels::fb::ColumnChunkIndex>> indexVec;
  indexVec.push_back(column_chunk_index_offset);
  auto indicesOffset = fbb.CreateVector(indexVec);
  auto rowGroupIndexOffset = pixels::fb::CreateRowGroupIndex(fbb, indicesOffset);

  std::vector<flatbuffers::Offset<pixels::fb::ColumnEncoding>> encodingVec;
  encodingVec.push_back(column_chunk_encoding_offset);
  auto encodingsOffset = fbb.CreateVector(encodingVec);
  auto rowGroupEncodingOffset = pixels::fb::CreateRowGroupEncoding(fbb, encodingsOffset);

  auto footerOffset = pixels::fb::CreateRowGroupFooter(fbb, rowGroupIndexOffset, rowGroupEncodingOffset);
  fbb.Finish(footerOffset);

  auto footer = flatbuffers::GetRoot<pixels::fb::RowGroupFooter>((fbb.GetBufferPointer()));
  auto column_chunk_encoding = footer->rowGroupEncoding()->columnChunkEncodings()->Get(0);
  auto column_chunk_index = footer->rowGroupIndexEntry()->columnChunkIndexEntries()->Get(0);

  auto long_result_vector =
      std::make_shared<LongColumnVector>(len, encoding, is_long);
  auto bit_mask = std::make_shared<PixelsBitMask>(len);
  auto num_to_read = len;
  auto pixel_offset = 0;
  auto vector_index = 0;
  while (num_to_read) {
    auto size = std::min(pixel_stride, num_to_read);
    long_column_reader->read(buffer, column_chunk_encoding, pixel_offset, size,
                             pixel_stride, vector_index, long_result_vector,
                             column_chunk_index,
                             bit_mask);
    for (int i = vector_index; i < vector_index + size; i++) {
      std::cerr << "[DEBUG READ CASE1] " << long_result_vector->longVector[i]
                << std::endl;
      EXPECT_EQ(long_result_vector->longVector[i],
                long_column_vector->longVector[i]);
    }
    pixel_offset += size;
    vector_index += size;
    num_to_read -= size;
  }

  long_column_writer->close();
}

TEST(IntWriterTest, WriteRunLengthEncodeIntWithNull) {
  int len = 23;
  int pixel_stride = 5;
  bool encoding = true;
  auto integer_column_vector =
      std::make_shared<IntColumnVector>(len, encoding);
  ASSERT_TRUE(integer_column_vector);
  for (int i = 0; i < len; ++i) {
    if (i % 2) {
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

  flatbuffers::FlatBufferBuilder fbb(1024);

  auto integer_column_writer = std::make_unique<IntColumnWriter>(
      TypeDescription::createInt(), option);
  auto write_size = integer_column_writer->write( integer_column_vector, len);
  EXPECT_NE(write_size, 0);
  integer_column_writer->flush();
  auto content = integer_column_writer->getColumnChunkContent();
  EXPECT_GT(content.size(), 0);

  std::cerr << "[DEBUG] content size: " << content.size() << std::endl;

  /**----------------------
   **      Write End. Use Reader to check
   *------------------------**/
  auto integer_column_reader =
      std::make_unique<IntColumnReader>(TypeDescription::createInt());
  auto buffer = std::make_shared<ByteBuffer>(content.size());
  buffer->putBytes(content.data(), content.size());
  
  auto column_chunk_encoding_offset = integer_column_writer->getColumnChunkEncoding(fbb);
  auto column_chunk_index_offset = integer_column_writer->buildColumnChunkIndex(fbb, 0, content.size(), true);

  std::vector<flatbuffers::Offset<pixels::fb::ColumnChunkIndex>> indexVec;
  indexVec.push_back(column_chunk_index_offset);
  auto indicesOffset = fbb.CreateVector(indexVec);
  auto rowGroupIndexOffset = pixels::fb::CreateRowGroupIndex(fbb, indicesOffset);

  std::vector<flatbuffers::Offset<pixels::fb::ColumnEncoding>> encodingVec;
  encodingVec.push_back(column_chunk_encoding_offset);
  auto encodingsOffset = fbb.CreateVector(encodingVec);
  auto rowGroupEncodingOffset = pixels::fb::CreateRowGroupEncoding(fbb, encodingsOffset);

  auto footerOffset = pixels::fb::CreateRowGroupFooter(fbb, rowGroupIndexOffset, rowGroupEncodingOffset);
  fbb.Finish(footerOffset);

  auto footer = flatbuffers::GetRoot<pixels::fb::RowGroupFooter>((fbb.GetBufferPointer()));
  auto column_chunk_encoding = footer->rowGroupEncoding()->columnChunkEncodings()->Get(0);
  auto column_chunk_index = footer->rowGroupIndexEntry()->columnChunkIndexEntries()->Get(0);

  auto int_result_vector =
      std::make_shared<IntColumnVector>(len, encoding);
  auto bit_mask = std::make_shared<PixelsBitMask>(len);

  auto num_to_read = len;
  auto pixel_offset = 0;
  auto vector_index = 0;
  while (num_to_read) {
    auto size = std::min(pixel_stride, num_to_read);
    integer_column_reader->read(
        buffer, column_chunk_encoding, pixel_offset, size, pixel_stride,
        vector_index, int_result_vector,
        column_chunk_index, bit_mask);
    for (int i = vector_index; i < vector_index + size; i++) {
      std::cerr << "[DEBUG READ CASE1] " << int_result_vector->intVector[i]
                << std::endl;
    }
    pixel_offset += size;
    vector_index += size;
    num_to_read -= size;
  }
  integer_column_writer->close();
}
