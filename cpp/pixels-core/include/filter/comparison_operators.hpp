#pragma once
namespace pixels{
    //仿照duckdb的比较符结构体，每个结构体都有一个静态函数Operation，接受两个参数，返回比较结果。PixelsFilter.cpp中会调用这个函数来进行过滤操作。

struct Equals {
	template <class T>
	static inline bool Operation(const T &left, const T &right) {
		return left == right;
	}
};
struct NotEquals {
	template <class T>
	static inline bool Operation(const T &left, const T &right) {
		return !Equals::Operation(left, right);
	}
};

struct GreaterThan {
	template <class T>
	static inline bool Operation(const T &left, const T &right) {
		return left > right;
	}
};

struct GreaterThanEquals {
	template <class T>
	static inline bool Operation(const T &left, const T &right) {
		return !GreaterThan::Operation(right, left);
	}
};

struct LessThan {
	template <class T>
	static inline bool Operation(const T &left, const T &right) {
		return GreaterThan::Operation(right, left);
	}
};

struct LessThanEquals {
	template <class T>
	static inline bool Operation(const T &left, const T &right) {
		return !GreaterThan::Operation(left, right);
	}
};
}