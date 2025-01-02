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
#include "grpc/transpile_sql_client.h"

#include "exception/grpc_exception.h"
#include "exception/grpc_transpile_exception.h"
#include "exception/grpc_unavailable_exception.h"

using amphi::proto::AmphiService;
using amphi::proto::RequestHeader;
using amphi::proto::TranspileSqlRequest;
using amphi::proto::TranspileSqlResponse;
using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

TranspileSqlClient::TranspileSqlClient(std::shared_ptr <Channel> channel)
        : stub_(AmphiService::NewStub(channel))
{};

std::string TranspileSqlClient::TranspileSql(const std::string &token,
                                             const std::string &sql_statement,
                                             const std::string &from_dialect,
                                             const std::string &to_dialect)
{
    // Prepare the request
    TranspileSqlRequest request;
    request.mutable_header()->set_token(token);
    request.set_sqlstatement(sql_statement);
    request.set_fromdialect(from_dialect);
    request.set_todialect(to_dialect);

    // Send the request and get the response
    TranspileSqlResponse response;
    ClientContext context;

    Status status = stub_->TranspileSql(&context, request, &response);

    if (status.ok())
    {
        if (response.header().errorcode() == 0)
        {
            return response.sqltranspiled();
        }
        else
        {
            throw GrpcTranspileException(response.header().errormsg());
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
