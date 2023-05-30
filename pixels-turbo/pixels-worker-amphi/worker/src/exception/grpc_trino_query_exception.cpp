#include "exception/grpc_trino_query_exception.h"

GrpcTrinoQueryException::GrpcTrinoQueryException(const std::string& message) : message_(message) {}

const char* GrpcTrinoQueryException::what() const noexcept {
    return message_.c_str();
}