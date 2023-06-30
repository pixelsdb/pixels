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
#include "grpc/coordinate_query_client.h"
#include "exception/grpc_exception.h"
#include "exception/grpc_coordinator_query_exception.h"
#include "exception/grpc_unavailable_exception.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using amphi::proto::AmphiService;
using amphi::proto::CoordinateQueryRequest;
using amphi::proto::CoordinateQueryResponse;
using amphi::proto::RequestHeader;

CoordinateQueryClient::CoordinateQueryClient(std::shared_ptr<Channel> channel)
        : stub_(AmphiService::NewStub(channel)) {};

CoordinateQueryResponse CoordinateQueryClient::CoordinateQuery(
        const std::string& token,
        const std::string& peer_name,
        const std::string& schema,
        const std::string& sql_statement){

    // Prepare the request
    CoordinateQueryRequest request;
    request.mutable_header()->set_token(token);
    request.set_peername(peer_name);
    request.set_schema(schema);
    request.set_sqlstatement(sql_statement);

    // Send the request and get the response
    CoordinateQueryResponse response;
    ClientContext context;

    Status status = stub_->CoordinateQuery(&context, request, &response);

    if (status.ok()) {
        if (response.header().errorcode() == 0) {
            return response;
        } else {
            throw GrpcCoordinatorQueryException(response.header().errormsg());
        }
    } else if (status.error_code() == grpc::StatusCode::UNAVAILABLE) {
        throw GrpcUnavailableException(status.error_message());
    } else {
        throw GrpcException(status.error_message());
    }
}

