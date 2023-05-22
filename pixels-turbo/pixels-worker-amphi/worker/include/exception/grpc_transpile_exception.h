#ifndef PIXELS_WORKER_AMPHI_GRPC_TRANSPILE_EXCEPTION_H
#define PIXELS_WORKER_AMPHI_GRPC_TRANSPILE_EXCEPTION_H

#include <exception>
#include <iostream>
#include <string>

class GrpcTranspileException: public std::exception {
private:
    std::string message_;
public:
    GrpcTranspileException(const std::string& message);

    const char* what() const noexcept override;
};

#endif //PIXELS_WORKER_AMPHI_GRPC_TRANSPILE_EXCEPTION_H
