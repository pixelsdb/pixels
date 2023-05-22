#include "exception/grpc_transpile_exception.h"

GrpcTranspileException::GrpcTranspileException(const std::string& message) : message_(message) {}

const char* GrpcTranspileException::what() const noexcept {
    return message_.c_str();
}