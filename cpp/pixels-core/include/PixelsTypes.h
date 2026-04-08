#pragma once
//放一些枚举类

#ifndef PIXELS_TYPES_H
#define PIXELS_TYPES_H
#include <variant>
#include <cstdint>
#include <cassert>
#include <string>
#include <cstring>
using idx_t = uint64_t;
namespace pixels {

enum class PhysicalType : uint8_t {//decimal的物理存储类型
    INT16,
    INT32,
    INT64,
    INT128,
};

struct DecimalConfig {//decimal不同物理存储类型的最大宽度
        static constexpr int MAX_WIDTH_INT16 = 4;
        static constexpr int MAX_WIDTH_INT32 = 9;
        static constexpr int MAX_WIDTH_INT64 = 18;
        static constexpr int MAX_WIDTH_INT128 = 38;
};

enum class ComparisonOperator : uint8_t {
        EQUAL,
        NOT_EQUAL,
        GREATER_THAN,
        GREATER_THAN_OR_EQUAL,
        LESS_THAN,
        LESS_THAN_OR_EQUAL,
        IS_NULL,
        IS_NOT_NULL
};

enum class TableFilterType : uint8_t {
	CONSTANT_COMPARISON = 0, // constant comparison (e.g. =C, >C, >=C, <C, <=C)
	IS_NULL = 1,             // C IS NULL 不支持
	IS_NOT_NULL = 2,         // C IS NOT NULL 不支持
	CONJUNCTION_OR = 3,      // OR of different filters
	CONJUNCTION_AND = 4,     // AND of different filters
	STRUCT_EXTRACT = 5,      // filter applies to child-column of struct 不支持
	OPTIONAL_FILTER = 6,     // executing filter is not required for query correctness 不支持
	IN_FILTER = 7,           // col IN (C1, C2, C3, ...)    不支持
	DYNAMIC_FILTER = 8,      // dynamic filters can be updated at run-time  不支持
	EXPRESSION_FILTER = 9,   // an arbitrary expression     不支持
    DEFAULT=10               //未定义
};

struct Scalar {
    enum class Type {
        INT32,
        INT64,
        FLOAT,
        DOUBLE,
        STRING,
        INVALID
    };

private:
    Type type_;

    int32_t i32_;
    int64_t i64_;
    float f_;
    double d_;
    std::string str_;

public:
    // ---------- 构造 ----------
    Scalar() : type_(Type::INVALID) {}

    Scalar(int32_t v) : type_(Type::INT32), i32_(v) {}
    Scalar(int64_t v) : type_(Type::INT64), i64_(v) {}
    Scalar(float v) : type_(Type::FLOAT), f_(v) {}
    Scalar(double v) : type_(Type::DOUBLE), d_(v) {}
    Scalar(const std::string &v) : type_(Type::STRING), str_(v) {}
    Scalar(std::string &&v) : type_(Type::STRING), str_(std::move(v)) {}

    // ---------- 类型访问 ----------
    Type type() const {
        return type_;
    }

    bool is_int32() const { return type_ == Type::INT32; }
    bool is_int64() const { return type_ == Type::INT64; }
    bool is_float() const { return type_ == Type::FLOAT; }
    bool is_double() const { return type_ == Type::DOUBLE; }
    bool is_string() const { return type_ == Type::STRING; }

    // ---------- 读取接口 ----------
    int32_t get_int32() const {
        assert(type_ == Type::INT32);
        return i32_;
    }

    int64_t get_int64() const {
        assert(type_ == Type::INT64);
        return i64_;
    }

    float get_float() const {
        assert(type_ == Type::FLOAT);
        return f_;
    }

    double get_double() const {
        assert(type_ == Type::DOUBLE);
        return d_;
    }

    const std::string &get_string() const {
        assert(type_ == Type::STRING);
        return str_;
    }

    // ---------- 写入接口 ----------
    void set(int32_t v) {
        type_ = Type::INT32;
        i32_ = v;
    }

    void set(int64_t v) {
        type_ = Type::INT64;
        i64_ = v;
    }

    void set(float v) {
        type_ = Type::FLOAT;
        f_ = v;
    }

    void set(double v) {
        type_ = Type::DOUBLE;
        d_ = v;
    }

    void set(const std::string &v) {
        type_ = Type::STRING;
        str_ = v;
    }

    void set(std::string &&v) {
        type_ = Type::STRING;
        str_ = std::move(v);
    }
};

struct string_t {
public:
    static constexpr uint32_t INLINE_LENGTH = 12;
    string_t() {
        value.inlined.length = 0;
    }
    string_t(const char *data, uint32_t len) {
        value.inlined.length = len;
        if (IsInlined()) {
            // 小字符串：直接拷贝
            memset(value.inlined.inlined, 0, INLINE_LENGTH);
            if (len > 0) {
                memcpy(value.inlined.inlined, data, len);
            }
        } else {
            // 大字符串：存指针 + prefix
            memcpy(value.pointer.prefix, data, 4);
            value.pointer.ptr = const_cast<char *>(data);
        }
    }

    string_t(const std::string &str)
        : string_t(str.data(), (uint32_t)str.size()) {}

    inline bool IsInlined() const {
        return GetSize() <= INLINE_LENGTH;
    }

    inline const char *GetData() const {
        return IsInlined() ? value.inlined.inlined : value.pointer.ptr;
    }

    inline uint32_t GetSize() const {
        return value.inlined.length;
    }

    inline std::string ToString() const {
        return std::string(GetData(), GetSize());
    }

private:
    union {
        struct {
            uint32_t length;
            char prefix[4];
            char *ptr;
        } pointer;

        struct {
            uint32_t length;
            char inlined[INLINE_LENGTH];
        } inlined;
    } value;
};


} // namespace pixels

#endif // PIXELS_TYPES_H