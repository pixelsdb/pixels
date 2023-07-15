//
// Created by liyu on 3/16/23.
//

#include "TypeDescription.h"

#include <utility>
#include "exception/InvalidArgumentException.h"


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
            default:
                throw InvalidArgumentException("Unknown type: " + type->name());
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
        case STRING:
        case BINARY:
        case VARBINARY:
        case CHAR:
        case VARCHAR: {
		    return std::make_shared<BinaryColumnVector>(maxSize, useEncodedVector.at(0));
//		    if (!useEncodedVector.at(0)) {
//			    return std::make_shared<BinaryColumnVector>(maxSize);
//		    } else {
//			    // TODO: dict should be supported here
//			    assert(false);
//		    }
	    }
        default:
            throw InvalidArgumentException("TypeDescription: Unknown type when creating column");
    }
}

TypeDescription::Category TypeDescription::getCategory() {
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

