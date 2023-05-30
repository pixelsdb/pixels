#include "grpc/trino_query_client.h"
#include "exception/grpc_exception.h"
#include "exception/grpc_trino_query_exception.h"
#include "exception/grpc_unavailable_exception.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using amphi::proto::AmphiService;
using amphi::proto::TrinoQueryRequest;
using amphi::proto::TrinoQueryResponse;
using amphi::proto::RequestHeader;

TrinoQueryClient::TrinoQueryClient(std::shared_ptr<Channel> channel)
        : stub_(AmphiService::NewStub(channel)) {}

std::string TrinoQueryClient::TrinoQuery(
        const std::string& token,
        const std::string& trino_url,
        const int& trino_port,
        const std::string& catalog,
        const std::string& schema,
        const std::string& sql_query){

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

    if (status.ok()) {
        if (response.header().errorcode() == 0) {
            return response.queryresult();
        } else {
            throw GrpcTrinoQueryException(response.header().errormsg());
        }
    } else if (status.error_code() == grpc::StatusCode::UNAVAILABLE) {
        throw GrpcUnavailableException(status.error_message());
    } else {
        throw GrpcException(status.error_message());
    }
}
