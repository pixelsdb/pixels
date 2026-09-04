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
 * @create 2023-03-16
 */
#ifndef PIXELS_TYPEDESCRIPTION_H
#define PIXELS_TYPEDESCRIPTION_H

/**
 * TypeDescription derived from org.apache.orc
 * <p>
 * Schema description in a Pixels file.
 */

#include <iostream>
#include <vector>
#include <string>
#include <memory>
#include <map>
#include <span>

#include "pixels_generated.h"
#include <vector/VectorizedRowBatch.h>
#include "vector/LongColumnVector.h"
#include "vector/ByteColumnVector.h"
#include "vector/BinaryColumnVector.h"
#include "vector/DecimalColumnVector.h"
#include "vector/DateColumnVector.h"
#include "vector/TimestampColumnVector.h"
#include "vector/IntColumnVector.h"

struct CategoryProperty
{
    bool isPrimitive;
    std::vector <std::string> names;
};

class TypeDescription : public std::enable_shared_from_this<TypeDescription>
{
public:
    enum Category
    {
        BOOLEAN,
        BYTE,
        SHORT,
        INT,
        LONG,
        FLOAT,
        DOUBLE,
        DECIMAL,
        STRING,
        DATE,
        TIME,
        TIMESTAMP,
        VARBINARY,
        BINARY,
        VARCHAR,
        CHAR,
        STRUCT
    };

    class StringPosition
    {
        friend class TypeDescription;
    public:
        StringPosition(const std::string &value);

        std::string toString() const;

    public:
        std::string value;
        int position;
        int length;
    };

    TypeDescription() = default;

    TypeDescription(Category c);

    static std::shared_ptr <TypeDescription> createBoolean();

    static std::shared_ptr <TypeDescription> createByte();

    static std::shared_ptr <TypeDescription> createShort();

    static std::shared_ptr <TypeDescription> createInt();

    static std::shared_ptr <TypeDescription> createLong();

    static std::shared_ptr <TypeDescription> createFloat();

    static std::shared_ptr <TypeDescription> createDouble();

    static std::shared_ptr <TypeDescription> createDecimal(int precision, int scale);

    static std::shared_ptr <TypeDescription> createString();

    static std::shared_ptr <TypeDescription> createDate();

    static std::shared_ptr <TypeDescription> createTime();

    static std::shared_ptr <TypeDescription> createTimestamp();

    static std::shared_ptr <TypeDescription> createVarbinary();

    static std::shared_ptr <TypeDescription> createBinary();

    static std::shared_ptr <TypeDescription> createVarchar();

    static std::shared_ptr <TypeDescription> createChar();

    static std::shared_ptr <TypeDescription> createStruct();

    static std::shared_ptr <TypeDescription>
    createSchema(std::span<const pixels::fb::Type*> types);

    std::shared_ptr <TypeDescription>
    addField(const std::string &field, const std::shared_ptr <TypeDescription> &fieldType);

    void setParent(const std::shared_ptr <TypeDescription> &p);

    std::shared_ptr <VectorizedRowBatch> createRowBatch(int maxSize);

    std::shared_ptr <VectorizedRowBatch> createRowBatch(int maxSize, const std::vector<bool> &useEncodedVector);

    static void requireChar(StringPosition &source, char required);

    static bool consumeChar(StringPosition &source, char ch);

    static int parseInt(StringPosition &source);

    static std::string parseName(StringPosition &source);

    static void parseStruct(std::shared_ptr <TypeDescription> type, StringPosition &source);

    static Category parseCategory(StringPosition &source);

    static std::shared_ptr <TypeDescription> parseType(StringPosition &source);

    static std::shared_ptr <TypeDescription> fromString(const std::string &typeName);

    std::vector <std::shared_ptr<TypeDescription>> getChildren();

    Category getCategory() const;

    std::vector <std::string> getFieldNames();

    int getPrecision();

    int getScale();

    TypeDescription withPrecision(int precision);

    TypeDescription withScale(int scale);

    TypeDescription withMaxLength(int maxLength);

    int getMaxLength();

    static std::map <Category, CategoryProperty> categoryMap;

    static int SHORT_DECIMAL_MAX_PRECISION;
    static int LONG_DECIMAL_MAX_PRECISION;

    static int SHORT_DECIMAL_MAX_SCALE;
    static int LONG_DECIMAL_MAX_SCALE;

    static int SHORT_DECIMAL_DEFAULT_PRECISION;
    static int LONG_DECIMAL_DEFAULT_PRECISION;

    static int DEFAULT_DECIMAL_SCALE;
    static int SHORT_DECIMAL_DEFAULT_SCALE;
    static int LONG_DECIMAL_DEFAULT_SCALE;

    static int DEFAULT_LENGTH;

    static int DEFAULT_CHAR_LENGTH;

    static int DEFAULT_TIMESTAMP_PRECISION;

    static int DEFAULT_TIME_PRECISION;

    static int MAX_TIMESTAMP_PRECISION;

    static int MAX_TIME_PRECISION;
    std::vector<flatbuffers::Offset<pixels::fb::Type>> writeTypes(flatbuffers::FlatBufferBuilder& fbb);


private:
    std::shared_ptr <ColumnVector> createColumn(int maxSize, bool useEncodedVector);

    std::shared_ptr <ColumnVector> createColumn(int maxSize, std::vector<bool> useEncodedVector);

    static long serialVersionUID;
    int id;
    int maxId;

    // here we use weak_ptr to avoid cyclic reference
    std::weak_ptr <TypeDescription> parent;
    Category category;
    std::vector <std::shared_ptr<TypeDescription>> children;
    std::vector <std::string> fieldNames;
    uint32_t maxLength;
    uint32_t precision;
    uint32_t scale;
};
#endif //PIXELS_TYPEDESCRIPTION_H
