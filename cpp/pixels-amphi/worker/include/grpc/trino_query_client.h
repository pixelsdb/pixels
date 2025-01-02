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
#ifndef PIXELS_WORKER_AMPHI_TRINO_QUERY_CLIENT_H
#define PIXELS_WORKER_AMPHI_TRINO_QUERY_CLIENT_H

#include <iostream>
#include <memory>
#include <string>

#include <grpc++/grpc++.h>
#include "amphi.pb.h"
#include "amphi.grpc.pb.h"

class TrinoQueryClient
{
public:
    TrinoQueryClient(std::shared_ptr <grpc::Channel> channel);

    std::string TrinoQuery(
            const std::string &token,
            const std::string &trino_url,
            const int &trino_port,
            const std::string &catalog,
            const std::string &schema,
            const std::string &sql_query);

private:
    std::unique_ptr <amphi::proto::AmphiService::Stub> stub_;
};

#endif //PIXELS_WORKER_AMPHI_TRINO_QUERY_CLIENT_H
