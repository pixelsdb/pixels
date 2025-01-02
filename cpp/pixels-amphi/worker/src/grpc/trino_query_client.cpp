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
#include "grpc/trino_query_client.h"

#include "exception/grpc_exception.h"
#include "exception/grpc_trino_query_exception.h"
#include "exception/grpc_unavailable_exception.h"

using amphi::proto::AmphiService;
using amphi::proto::RequestHeader;
using amphi::proto::TrinoQueryRequest;
using amphi::proto::TrinoQueryResponse;
using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

TrinoQueryClient::TrinoQueryClient(std::shared_ptr <Channel> channel)
        : stub_(AmphiService::NewStub(channel))
{};

std::string TrinoQueryClient::TrinoQuery(const std::string &token,
                                         const std::string &trino_url,
                                         const int &trino_port,
                                         const std::string &catalog,
                                         const std::string &schema,
                                         const std::string &sql_query)
{
    // Prepare the request
    TrinoQueryRequest request;
    request.mutable_header()->set_token(token);
    request.set_trinourl(trino_url);
    request.set_trinoport(trino_port);
    request.set_catalog(catalog);
    request.set_schema(schema);
    request.set_sqlquery(sql_query);

    // Send the request and get the response
    TrinoQueryResponse response;
    ClientContext context;

    Status status = stub_->TrinoQuery(&context, request, &response);

    if (status.ok())
    {
        if (response.header().errorcode() == 0)
        {
            return response.queryresult();
        }
        else
        {
            throw GrpcTrinoQueryException(response.header().errormsg());
        }
    }
    else if (status.error_code() == grpc::StatusCode::UNAVAILABLE)
    {
        throw GrpcUnavailableException(status.error_message());
    }
    else
    {
        throw GrpcException(status.error_message());
    }
}
