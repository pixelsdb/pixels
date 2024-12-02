//
// Created by liyu on 3/16/23.
//

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
#include <vector/VectorizedRowBatch.h>
#include "vector/LongColumnVector.h"
#include "vector/ByteColumnVector.h"
#include "vector/BinaryColumnVector.h"
#include "vector/DecimalColumnVector.h"
#include "vector/DateColumnVector.h"
#include "vector/TimestampColumnVector.h"
#include "pixels-common/pixels.pb.h"

struct CategoryProperty {
    bool isPrimitive;
    std::vector<std::string> names;
};

class TypeDescription: public std::enable_shared_from_this<TypeDescription> {
public:
    enum Category {
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
    class StringPosition {
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
    static std::shared_ptr<TypeDescription> createBoolean();
    static std::shared_ptr<TypeDescription> createByte();
    static std::shared_ptr<TypeDescription> createShort();
    static std::shared_ptr<TypeDescription> createInt();
    static std::shared_ptr<TypeDescription> createLong();
    static std::shared_ptr<TypeDescription> createFloat();
    static std::shared_ptr<TypeDescription> createDouble();
	static std::shared_ptr<TypeDescription> createDecimal(int precision, int scale);
    static std::shared_ptr<TypeDescription> createString();
    static std::shared_ptr<TypeDescription> createDate();
    static std::shared_ptr<TypeDescription> createTime();
    static std::shared_ptr<TypeDescription> createTimestamp();
    static std::shared_ptr<TypeDescription> createVarbinary();
    static std::shared_ptr<TypeDescription> createBinary();
    static std::shared_ptr<TypeDescription> createVarchar();
    static std::shared_ptr<TypeDescription> createChar();
    static std::shared_ptr<TypeDescription> createStruct();
    static std::shared_ptr<TypeDescription> createSchema(const std::vector<std::shared_ptr<pixels::proto::Type>>& types);
    std::shared_ptr<TypeDescription> addField(const std::string& field, const std::shared_ptr<TypeDescription>& fieldType);
    void setParent(const std::shared_ptr<TypeDescription>& p);
	std::shared_ptr<VectorizedRowBatch> createRowBatch(int maxSize);
    std::shared_ptr<VectorizedRowBatch> createRowBatch(int maxSize, const std::vector<bool>& useEncodedVector);
    static void requireChar(StringPosition &source, char required);
    static bool consumeChar(StringPosition &source, char ch);
    static int parseInt(StringPosition &source);
    static std::string parseName(StringPosition &source);
    static void parseStruct(std::shared_ptr<TypeDescription> type, StringPosition &source);
    static Category parseCategory(StringPosition &source);
    static std::shared_ptr<TypeDescription> parseType(StringPosition &source);
    static std::shared_ptr<TypeDescription> fromString(const std::string &typeName);
    std::vector<std::shared_ptr<TypeDescription>> getChildren();
    Category getCategory() const;
	std::vector<std::string> getFieldNames();
	int getPrecision();
	int getScale();
    TypeDescription withPrecision(int precision);
    TypeDescription withScale(int scale);
    TypeDescription withMaxLength(int maxLength);

    int getMaxLength();
    static std::map<Category, CategoryProperty> categoryMap;

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

    void writeTypes(std::shared_ptr<pixels::proto::Footer> footer);



private:
    std::shared_ptr<ColumnVector> createColumn(int maxSize, bool useEncodedVector);
    std::shared_ptr<ColumnVector> createColumn(int maxSize, std::vector<bool> useEncodedVector);
    static long serialVersionUID;
    int id;
    int maxId;

	// here we use weak_ptr to avoid cyclic reference
    std::weak_ptr<TypeDescription> parent;
    Category category;
    std::vector<std::shared_ptr<TypeDescription>> children;
    std::vector<std::string> fieldNames;
    uint32_t maxLength;
    uint32_t precision;
    uint32_t scale;
};
#endif //PIXELS_TYPEDESCRIPTION_H
