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

//
// Created by liyu on 3/16/23.
//

#include "TypeDescription.h"

#include <utility>
#include "exception/InvalidArgumentException.h"
#include <sstream>
#include <regex>

long TypeDescription::serialVersionUID = 4270695889340023552L;

/**
 * In SQL standard, the max precision of decimal is 38.
 * Issue #196: support short decimal of which the max precision is 18.
 * Issue #203: support long decimal of which the max precision is 38.
 */
int TypeDescription::SHORT_DECIMAL_MAX_PRECISION = 18;
int TypeDescription::LONG_DECIMAL_MAX_PRECISION = 38;

/**
 * In SQL standard, the max scale of decimal is 38.
 * Issue #196: support short decimal of which the max scale is 18.
 * Issue #203: support long decimal of which the max scale is 38.
 */
int TypeDescription::SHORT_DECIMAL_MAX_SCALE = 18;
int TypeDescription::LONG_DECIMAL_MAX_SCALE = 38;
/**
 * In SQL standard, the default precision of decimal is 38.
 * Issue #196: support short decimal of which the default precision is 18.
 * Issue #203: support long decimal of which the default precision is 38.
 */
int TypeDescription::SHORT_DECIMAL_DEFAULT_PRECISION = 18;
int TypeDescription::LONG_DECIMAL_DEFAULT_PRECISION = 38;
/**
 * It is a standard that the default scale of decimal is 0.
 */
int TypeDescription::DEFAULT_DECIMAL_SCALE = 0;
int TypeDescription::SHORT_DECIMAL_DEFAULT_SCALE = 0;
int TypeDescription::LONG_DECIMAL_DEFAULT_SCALE = 0;
/**
 * The default length of varchar, binary, and varbinary.
 */
int TypeDescription::DEFAULT_LENGTH = 65535;
/**
 * It is a standard that the default length of char is 1.
 */
int TypeDescription::DEFAULT_CHAR_LENGTH = 1;
/**
 * In SQL standard, the default precision of timestamp is 6 (i.e., microseconds),
 * however, in Pixels, we use default precision 3 to be compatible with Trino.
 */
int TypeDescription::DEFAULT_TIMESTAMP_PRECISION = 3;
/**
 * 9 = nanosecond, 6 = microsecond, 3 = millisecond, 0 = second.
 * <p>Although 64-bit long is enough to encode the nanoseconds for now, it is
 * risky to encode a nanosecond time in the near future, thus some query engines
 * such as Trino only use long to encode a timestamp up to 6 precision.
 * Therefore, we also set the max precision of long encoded timestamp to 6 in Pixels.</p>
 */
int TypeDescription::MAX_TIMESTAMP_PRECISION = 6;
/**
 * In SQL standard, the default precision of time is 6 (i.e., microseconds), however,
 * in Pixels, we use the default precision 3 (i.e., milliseconds), which is consistent with Trino.
 */
 int TypeDescription::DEFAULT_TIME_PRECISION = 3;
/**
 * 9 = nanosecond, 6 = microsecond, 3 = millisecond, 0 = second.
 * <p>In Pixels, we use 32-bit integer to store time, thus we can support time precision up to 4.
 * For simplicity and compatibility to {@link java.sql.Time}, we further limit the precision to 3.</p>
 */
 int TypeDescription::MAX_TIME_PRECISION = 3;

TypeDescription::StringPosition::StringPosition(const std::string &value)
    : value(value), position(0), length(value.size()) {}

std::string TypeDescription::StringPosition::toString() const {
    std::ostringstream buffer;
    buffer << '\'';
    buffer << value.substr(0, position);
    buffer << '^';
    buffer << value.substr(position);
    buffer << '\'';
    return buffer.str();
}

std::map<TypeDescription::Category, CategoryProperty> TypeDescription::categoryMap = {
        {BOOLEAN, {true, {"boolean"}}},
        {BYTE, {true, {"tinyint", "byte"}}},
        {SHORT, {true, {"smallint", "short"}}},
        {INT, {true, {"integer", "int"}}},
        {LONG, {true, {"bigint", "long"}}},
        {FLOAT, {true, {"float", "real"}}},
        {DOUBLE, {true, {"double"}}},
        {DECIMAL, {true, {"decimal"}}},
        {STRING, {true, {"string"}}},
        {DATE, {true, {"date"}}},
        {TIME, {true, {"time"}}},
        {TIMESTAMP, {true, {"timestamp"}}},
        {VARBINARY, {true, {"varbinary"}}},
        {BINARY, {true, {"binary"}}},
        {VARCHAR, {true, {"varchar"}}},
        {CHAR, {true, {"char"}}},
        {STRUCT, {false, {"struct"}}}
};

TypeDescription::TypeDescription(Category c) {
    id = -1;
    maxId = -1;
    maxLength = DEFAULT_LENGTH;
    precision = SHORT_DECIMAL_DEFAULT_PRECISION;
    scale = SHORT_DECIMAL_DEFAULT_SCALE;

    category = c;
}

std::shared_ptr<TypeDescription> TypeDescription::createSchema(const std::vector<std::shared_ptr<pixels::proto::Type>>& types) {
	std::shared_ptr<TypeDescription> schema = createStruct();
    for(const auto& type : types) {
        const std::string& fieldName = type->name();
        std::shared_ptr<TypeDescription> fieldType;
        switch (type->kind()) {
            case pixels::proto::Type_Kind_BOOLEAN:
                fieldType = TypeDescription::createBoolean();
                break;
            case pixels::proto::Type_Kind_LONG:
                fieldType = TypeDescription::createLong();
                break;
            case pixels::proto::Type_Kind_INT:
                fieldType = TypeDescription::createInt();
                break;
            case pixels::proto::Type_Kind_SHORT:
                fieldType = TypeDescription::createShort();
                break;
            case pixels::proto::Type_Kind_BYTE:
                fieldType = TypeDescription::createByte();
                break;
            case pixels::proto::Type_Kind_FLOAT:
                fieldType = TypeDescription::createFloat();
                break;
            case pixels::proto::Type_Kind_DOUBLE:
                fieldType = TypeDescription::createDouble();
                break;
		    case pixels::proto::Type_Kind_DECIMAL:
			    fieldType = TypeDescription::createDecimal(type->precision(), type->scale());
			    break;
            case pixels::proto::Type_Kind_VARCHAR:
                fieldType = TypeDescription::createVarchar();
                fieldType->maxLength = type->maximumlength();
                break;
            case pixels::proto::Type_Kind_CHAR:
                fieldType = TypeDescription::createChar();
                fieldType->maxLength = type->maximumlength();
                break;
            case pixels::proto::Type_Kind_STRING:
                fieldType = TypeDescription::createString();
                break;
            case pixels::proto::Type_Kind_DATE:
                fieldType = TypeDescription::createDate();
                break;
            case pixels::proto::Type_Kind_TIME:
                fieldType = TypeDescription::createTime();
                break;
            case pixels::proto::Type_Kind_TIMESTAMP:
                fieldType = TypeDescription::createTimestamp();
                break;
            default:
                throw InvalidArgumentException("TypeDescription::createSchema: Unknown type: " + type->name());
        }
        schema->addField(fieldName, fieldType);
    }
    return schema;
}

std::shared_ptr<TypeDescription> TypeDescription::createBoolean() {
	return std::make_shared<TypeDescription>(BOOLEAN);
}

std::shared_ptr<TypeDescription> TypeDescription::createByte() {
	return std::make_shared<TypeDescription>(BYTE);
}

std::shared_ptr<TypeDescription> TypeDescription::createShort() {
	return std::make_shared<TypeDescription>(SHORT);
}

std::shared_ptr<TypeDescription> TypeDescription::createInt() {
	return std::make_shared<TypeDescription>(INT);
}

std::shared_ptr<TypeDescription> TypeDescription::createLong() {
	return std::make_shared<TypeDescription>(LONG);
}

std::shared_ptr<TypeDescription> TypeDescription::createFloat() {
	return std::make_shared<TypeDescription>(FLOAT);
}

std::shared_ptr<TypeDescription> TypeDescription::createDouble() {
	return std::make_shared<TypeDescription>(DOUBLE);
}

std::shared_ptr<TypeDescription> TypeDescription::createDecimal(int precision, int scale) {
	auto type = std::make_shared<TypeDescription>(DECIMAL);
	type->precision = precision;
	type->scale = scale;
	return type;
}


std::shared_ptr<TypeDescription> TypeDescription::createString() {
	return std::make_shared<TypeDescription>(STRING);
}

std::shared_ptr<TypeDescription> TypeDescription::createDate() {
	return std::make_shared<TypeDescription>(DATE);
}

std::shared_ptr<TypeDescription> TypeDescription::createTime() {
	return std::make_shared<TypeDescription>(TIME);
}

std::shared_ptr<TypeDescription> TypeDescription::createTimestamp() {
	return std::make_shared<TypeDescription>(TIMESTAMP);
}

std::shared_ptr<TypeDescription> TypeDescription::createVarbinary() {
	return std::make_shared<TypeDescription>(VARBINARY);
}

std::shared_ptr<TypeDescription> TypeDescription::createBinary() {
	return std::make_shared<TypeDescription>(BINARY);
}

std::shared_ptr<TypeDescription> TypeDescription::createVarchar() {
	return std::make_shared<TypeDescription>(VARCHAR);
}

std::shared_ptr<TypeDescription> TypeDescription::createChar() {
	return std::make_shared<TypeDescription>(CHAR);
}

std::shared_ptr<TypeDescription> TypeDescription::createStruct() {
	return std::make_shared<TypeDescription>(STRUCT);
}

std::shared_ptr<TypeDescription> TypeDescription::addField(const std::string& field, const std::shared_ptr<TypeDescription>& fieldType) {
    if(category != STRUCT) {
        throw InvalidArgumentException("Can only add fields to struct type,"
                                       "but not " + categoryMap[category].names[0]);
    }
    fieldNames.emplace_back(field);
    children.emplace_back(fieldType);
    fieldType->setParent(shared_from_this());
    return shared_from_this();
}

void TypeDescription::setParent(const std::shared_ptr<TypeDescription>& p) {
    parent = p;
}

std::shared_ptr<VectorizedRowBatch> TypeDescription::createRowBatch(int maxSize) {
    return createRowBatch(maxSize, std::vector<bool>());
}
std::shared_ptr<VectorizedRowBatch> TypeDescription::createRowBatch(int maxSize, const std::vector<bool> &useEncodedVector) {
    std::shared_ptr<VectorizedRowBatch> result;
    if(category == STRUCT) {
        if(!(useEncodedVector.empty() || useEncodedVector.size() == children.size())) {
            throw InvalidArgumentException(
                    "There must be 0 or children.size() element in useEncodedVector");
        }
        result = std::make_shared<VectorizedRowBatch>(children.size(), maxSize);
        std::vector<std::string> columnNames;
        for(int i = 0; i < result->cols.size(); i++) {
            std::string fieldName = fieldNames.at(i);
            auto cv = children.at(i)->createColumn(
                    maxSize, !useEncodedVector.empty()
                    && useEncodedVector.at(i));
            // TODO: what if the duplication happens
            columnNames.emplace_back(fieldName);
            result->cols.at(i) = cv;
        }
    } else {
        if(!(useEncodedVector.empty() || useEncodedVector.size() == 1)) {
            throw InvalidArgumentException(
                    "For null structure type, There must be 0 or 1 element in useEncodedVector");
        }
        result = std::make_shared<VectorizedRowBatch>(1, maxSize);
        result->cols.at(0) = createColumn(
                maxSize, useEncodedVector.size() == 1
                && useEncodedVector[0]);
    }
    // TODO: reset the result
    return result;
}


std::vector<std::shared_ptr<TypeDescription>> TypeDescription::getChildren() {
    return children;
}

std::shared_ptr<ColumnVector> TypeDescription::createColumn(int maxSize, bool useEncodedVector) {
    return createColumn(maxSize, std::vector<bool>{useEncodedVector});
}


std::shared_ptr<ColumnVector> TypeDescription::createColumn(int maxSize, std::vector<bool> useEncodedVector) {
    assert(!useEncodedVector.empty());
    // the length of useEncodedVector is already checked, not need to check again.
    switch (category) {
        case SHORT:
        case INT:
			return std::make_shared<LongColumnVector>(maxSize, useEncodedVector.at(0), false);
        case LONG:
            return std::make_shared<LongColumnVector>(maxSize, useEncodedVector.at(0), true);
	    case DATE:
		    return std::make_shared<DateColumnVector>(maxSize, useEncodedVector.at(0));
	    case DECIMAL: {
		    if (precision <= SHORT_DECIMAL_MAX_PRECISION) {
				return std::make_shared<DecimalColumnVector>(maxSize, precision, scale, useEncodedVector.at(0));
		    } else {
				throw InvalidArgumentException("Currently we didn't implement LongDecimalColumnVector.");
		    }
	    }
        case TIMESTAMP:
            return std::make_shared<TimestampColumnVector>(maxSize, 0, useEncodedVector.at(0));
        case STRING:
            return std::make_shared<BinaryColumnVector>(maxSize, useEncodedVector.at(0));
        case BINARY:
        case VARBINARY:
        case CHAR:
        case VARCHAR: {
		    return std::make_shared<BinaryColumnVector>(maxSize, useEncodedVector.at(0));
	    }
        default:
            throw InvalidArgumentException("TypeDescription: Unknown type when creating column");
    }
}

TypeDescription::Category TypeDescription::getCategory() const {
    return category;
}

std::vector<std::string> TypeDescription::getFieldNames() {
	return fieldNames;
}

int TypeDescription::getPrecision() {
	return precision;
}

int TypeDescription::getScale() {
	return scale;
}
int TypeDescription::getMaxLength() {
    return maxLength;
}

void TypeDescription::requireChar(TypeDescription::StringPosition &source, char required) {
    if (source.position >= source.length || source.value[source.position] != required) {
        throw new InvalidArgumentException("Missing required char " + std::string(1, required) + " at " + source.toString());
    }
    source.position += 1;
}

bool TypeDescription::consumeChar(TypeDescription::StringPosition &source, char ch) {
    bool result = (source.position < source.length) && (source.value[source.position] == ch);
    if (result) {
        source.position += 1;
    }
    return result;
}

int TypeDescription::parseInt(TypeDescription::StringPosition &source) {
    int start = source.position;
    int result = 0;
    while (source.position < source.length) {
        char ch = source.value[source.position];
        if (!std::isdigit(ch)) {
            break;
        }
        result = result * 10 + (ch - '0');
        source.position += 1;
    }
    if (source.position == start) {
        throw new InvalidArgumentException("Missing integer at " + source.toString());
    }
    return result;
}

std::string TypeDescription::parseName(TypeDescription::StringPosition &source) {
    if (source.position == source.length) {
        throw new InvalidArgumentException("Missing name at " + source.toString());
    }
    int start = source.position;
    if (source.value[source.position] == '`') {
        source.position += 1;
        std::string buffer;
        bool closed = false;
        while (source.position < source.length) {
            char ch = source.value[source.position];
            source.position += 1;
            if (ch == '`') {
                if (source.position < source.length && source.value[source.position] == '`') {
                    source.position += 1;
                    buffer += '`';
                } else {
                    closed = true;
                    break;
                }
            } else {
                buffer += ch;
            }
        }
        if (!closed) {
            source.position = start;
            throw std::invalid_argument("Unmatched quote at " + source.toString());
        } else if (buffer.empty()) {
            throw new InvalidArgumentException("Empty quoted field name at " + source.toString());
        }
        return buffer;
    } else {
        while (source.position < source.length) {
            char ch = source.value[source.position];
            if (!std::isalnum(ch) && ch != '.' && ch != '_') {
                break;
            }
            source.position += 1;
        }
        if (source.position == start) {
            throw new InvalidArgumentException("Missing name at " + source.toString());
        }
        return source.value.substr(start, source.position - start);
    }
}

void TypeDescription::parseStruct(std::shared_ptr<TypeDescription> type, TypeDescription::StringPosition &source) {
    requireChar(source, '<');
    do {
        std::string fieldName = parseName(source);
        requireChar(source, ':');
        type->addField(fieldName, parseType(source));
    } while (consumeChar(source, ','));
    requireChar(source, '>');
}

TypeDescription::Category TypeDescription::parseCategory(TypeDescription::StringPosition &source) {
    int start = source.position;
    while (source.position < source.length && std::isalpha(source.value[source.position])) {
        ++source.position;
    }
    if (source.position != start) {
        std::string word = source.value.substr(start, source.position - start);
        std::transform(word.begin(), word.end(), word.begin(), ::tolower);
        for (const auto &entry : categoryMap) {
            for (const auto &name : entry.second.names) {
                if (word == name) {
                    return entry.first;
                }
            }
        }
    }
    throw std::invalid_argument("Can't parse type category at " + source.toString());
}

std::shared_ptr<TypeDescription> TypeDescription::parseType(TypeDescription::StringPosition &source) {
    auto result = std::make_shared<TypeDescription>(parseCategory(source));
    switch (result->getCategory()) {
        case BOOLEAN:
        case BYTE:
        case DATE:
        case DOUBLE:
        case FLOAT:
        case INT:
        case LONG:
        case SHORT:
        case STRING:
            break;
        case TIME:
            if (consumeChar(source, '(')) {
                result->withPrecision(parseInt(source));
                requireChar(source, ')');
            } else {
                result->withPrecision(DEFAULT_TIME_PRECISION);
            }
            break;
        case TIMESTAMP:
            if (consumeChar(source, '(')) {
                result->withPrecision(parseInt(source));
                requireChar(source, ')');
            } else {
                result->withPrecision(DEFAULT_TIMESTAMP_PRECISION);
            }
            break;
        case BINARY:
        case VARBINARY:
        case CHAR:
        case VARCHAR:
            if (consumeChar(source, '(')) {
                result->withMaxLength(parseInt(source));
                requireChar(source, ')');
            } else if (result->getCategory() == Category::CHAR) {
                result->withMaxLength(DEFAULT_CHAR_LENGTH);
            } else {
                result->withMaxLength(DEFAULT_LENGTH);
            }
            break;
        case DECIMAL:
            if (consumeChar(source, '(')) {
                int precision = parseInt(source);
                int scale = 0;
                if (consumeChar(source, ',')) {
                    scale = parseInt(source);
                }
                requireChar(source, ')');
                if (scale > precision) {
                    throw new InvalidArgumentException(std::string("Decimal's scale ") + std::to_string(scale) + " is greater than precision " + std::to_string(precision));
                }
                result->withPrecision(precision);
                result->withScale(scale);
            } else {
                result->withPrecision(LONG_DECIMAL_DEFAULT_PRECISION);
                result->withScale(DEFAULT_DECIMAL_SCALE);
            }
            break;
        case STRUCT:
            parseStruct(result, source);
            break;
        default:
            throw new InvalidArgumentException("Unknown type at " + source.toString());
    }
    return result;
}

std::shared_ptr<TypeDescription> TypeDescription::fromString(const std::string &typeName) {
    if (typeName == "") {
        return nullptr;
    }
    StringPosition source(std::regex_replace(typeName, std::regex("\\s+"), ""));
    auto result = parseType(source);
    if (source.position != source.length) {
        throw InvalidArgumentException(std::string("Extra characters at ") + source.toString());
    }
    return result;
}

TypeDescription TypeDescription::withPrecision(int precision) {
    if (this->category == Category::DECIMAL) {
        if (precision < 1 || precision > LONG_DECIMAL_MAX_PRECISION) {
            throw new InvalidArgumentException(std::string("precision ") + std::to_string(precision) + " is out of the valid range 1 .. " + std::to_string(LONG_DECIMAL_DEFAULT_PRECISION));
        } else if (scale > precision) {
            throw new InvalidArgumentException(std::string("precision ") + std::to_string(precision) + " is smaller that scale " + std::to_string(scale));
        }
    } else if (this->category == Category::TIMESTAMP) {
        if (precision < 0 || precision > MAX_TIMESTAMP_PRECISION) {
            throw new InvalidArgumentException(std::string("precision ") + std::to_string(precision) + " is out of the valid range 0 .. " + std::to_string(MAX_TIMESTAMP_PRECISION));
        }
    } else if (this->category == Category::TIME) {
        if (precision < 0 || precision > MAX_TIME_PRECISION) {
            throw new InvalidArgumentException(std::string("precision ") + std::to_string(precision) + " is out of the valid range 0 .. " + std::to_string(MAX_TIME_PRECISION));
        }
    } else {
        throw new InvalidArgumentException("precision is not valid on decimal, time, and timestamp.");
    }
    this->precision = precision;
    return *this;
}

TypeDescription TypeDescription::withScale(int scale) {
    if (this->category == Category::DECIMAL) {
        if (scale < 0 || scale > LONG_DECIMAL_MAX_SCALE) {
            throw new InvalidArgumentException(std::string("scale ") + std::to_string(scale) + " is out of the valid range 0 .. " + std::to_string(LONG_DECIMAL_MAX_SCALE));
        } else if (scale > precision) {
            throw new InvalidArgumentException(std::string("scale ") + std::to_string(scale) + " is out of the valid range 0 .. " + std::to_string(precision));
        }
    } else {
        throw new InvalidArgumentException("scale is only valid on decimal.");
    }
    this->scale = scale;
    return *this;
}

TypeDescription TypeDescription::withMaxLength(int maxLength) {
    if (this->category != Category::VARCHAR && this->category != Category::CHAR &&
        this->category != Category::BINARY && this->category != Category::VARBINARY) {
        throw new InvalidArgumentException(std::string("maxLength is only allowed on char, varchar, binary, and varbinary."));
    }
    if (maxLength < 1) {
        throw new InvalidArgumentException(std::string("maxLength ") + std::to_string(maxLength) + " is not positive");
    }
    this->maxLength = maxLength;
    return *this;
}

void TypeDescription::writeTypes(std::shared_ptr<pixels::proto::Footer> footer) {
    std::vector<std::shared_ptr<TypeDescription>> children= this->getChildren();
    std::vector<std::string> names=this->getFieldNames();
    if(children.empty()){
        return;
    }
    for(int i=0;i<children.size();i++){
        std::shared_ptr<TypeDescription> child=children.at(i);
        std::shared_ptr<pixels::proto::Type> tmpType=std::make_shared<pixels::proto::Type>();
        tmpType->set_name(names.at(i));
        switch (child->getCategory()) {
            case TypeDescription::Category::BOOLEAN:
                tmpType->set_kind(pixels::proto::Type_Kind::Type_Kind_BOOLEAN);
                break;
            case TypeDescription::Category::BYTE:
                tmpType->set_kind(pixels::proto::Type_Kind::Type_Kind_BYTE);
                break;
            case TypeDescription::Category::SHORT:
                tmpType->set_kind(pixels::proto::Type_Kind::Type_Kind_SHORT);
                break;
            case TypeDescription::Category::INT:
                tmpType->set_kind(pixels::proto::Type_Kind::Type_Kind_INT);
                break;
            case TypeDescription::Category::LONG:
                tmpType->set_kind(pixels::proto::Type_Kind::Type_Kind_LONG);
                break;
            case TypeDescription::Category::FLOAT:
                tmpType->set_kind(pixels::proto::Type_Kind::Type_Kind_FLOAT);
                break;
            case TypeDescription::Category::DOUBLE:
                tmpType->set_kind(pixels::proto::Type_Kind::Type_Kind_DOUBLE);
                break;
            case TypeDescription::Category::DECIMAL:
                tmpType->set_kind(pixels::proto::Type_Kind::Type_Kind_DECIMAL);
                tmpType->set_precision(child->getPrecision());
                tmpType->set_scale(child->getScale());
                break;
            case TypeDescription::Category::STRING:
                tmpType->set_kind(pixels::proto::Type_Kind::Type_Kind_STRING);
                break;
            case TypeDescription::Category::CHAR:
                tmpType->set_kind(pixels::proto::Type_Kind::Type_Kind_CHAR);
                tmpType->set_maximumlength(child->getMaxLength());
                break;
            case TypeDescription::Category::VARCHAR:
                tmpType->set_kind(pixels::proto::Type_Kind::Type_Kind_VARCHAR);
                tmpType->set_maximumlength(child->getMaxLength());
                break;
            case TypeDescription::Category::BINARY:
                tmpType->set_kind(pixels::proto::Type_Kind::Type_Kind_BINARY);
                tmpType->set_maximumlength(child->getMaxLength());
                break;
            case TypeDescription::Category::VARBINARY:
                tmpType->set_kind(pixels::proto::Type_Kind::Type_Kind_VARBINARY);
                tmpType->set_maximumlength(child->getMaxLength());
                break;
            case TypeDescription::Category::TIMESTAMP:
                tmpType->set_kind(pixels::proto::Type_Kind::Type_Kind_TIMESTAMP);
                tmpType->set_precision(child->getPrecision());
                break;
            case TypeDescription::Category::DATE:
                tmpType->set_kind(pixels::proto::Type_Kind::Type_Kind_DATE);
                break;
            case TypeDescription::Category::TIME:
                tmpType->set_kind(pixels::proto::Type_Kind::Type_Kind_TIME);
                tmpType->set_precision(child->getPrecision());
                break;
//            case TypeDescription::Category::VECTOR:
//                tmpType->set_kind(pixels::proto::Type_Kind::Type_Kind_VECTOR);
//                tmpType->set_dimension(child->getDimension());
//                break;
            default: {
                std::string errorMsg = "Unknown category: ";
                errorMsg += static_cast<std::underlying_type_t<Category>>(this->getCategory());
                throw std::runtime_error(errorMsg);
            }


        }
        *(footer->add_types())=*tmpType;
    }
}
