/*
 * Copyright 2023 PixelsDB.
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
