#ifndef PIXELS_WORKER_AMPHI_TRINO_QUERY_CLIENT_H
#define PIXELS_WORKER_AMPHI_TRINO_QUERY_CLIENT_H

#include <iostream>
#include <memory>
#include <string>

#include <grpc++/grpc++.h>
#include "amphi.pb.h"
#include "amphi.grpc.pb.h"

class TrinoQueryClient {
public:
    TrinoQueryClient(std::shared_ptr<grpc::Channel> channel);

    std::string TrinoQuery(
            const std::string& token,
            const std::string& trino_url,
            const int& trino_port,
            const std::string& catalog,
            const std::string& schema,
            const std::string& sql_query);

private:
    std::unique_ptr<amphi::proto::AmphiService::Stub> stub_;
};

#endif //PIXELS_WORKER_AMPHI_TRINO_QUERY_CLIENT_H
