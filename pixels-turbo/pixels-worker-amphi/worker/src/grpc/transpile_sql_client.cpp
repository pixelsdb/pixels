#include "grpc/transpile_sql_client.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using amphi::proto::AmphiService;
using amphi::proto::TranspileSqlRequest;
using amphi::proto::TranspileSqlResponse;
using amphi::proto::RequestHeader;

TranspileSqlClient::TranspileSqlClient(std::shared_ptr<Channel> channel)
        : stub_(AmphiService::NewStub(channel)) {}

std::string TranspileSqlClient::TranspileSql(const std::string& token, const std::string& sql_statement,
                                             const std::string& from_dialect, const std::string& to_dialect) {
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

    if (status.ok()) {
        return response.sqltranspiled();
    } else {
        std::cout << "TranspileSql failed: " << status.error_message() << std::endl;
        return "";
    }
}

