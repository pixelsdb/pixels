#include "exception/grpc_unavailable_exception.h"

GrpcUnavailableException::GrpcUnavailableException(const std::string& message) : message_(message) {}

const char* GrpcUnavailableException::what() const noexcept {
return message_.c_str();
}