#include "exception/grpc_exception.h"

GrpcException::GrpcException(const std::string& message) : message_(message) {}

const char* GrpcException::what() const noexcept {
    return message_.c_str();
}