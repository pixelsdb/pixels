#ifndef PIXELS_WORKER_AMPHI_TRANSPILE_SQL_CLIENT_H
#define PIXELS_WORKER_AMPHI_TRANSPILE_SQL_CLIENT_H

#include <iostream>
#include <memory>
#include <string>

#include <grpc++/grpc++.h>
#include "amphi.pb.h"
#include "amphi.grpc.pb.h"

class TranspileSqlClient {
public:
    TranspileSqlClient(std::shared_ptr<grpc::Channel> channel);

    std::string TranspileSql(const std::string& token, const std::string& sql_statement,
                             const std::string& from_dialect, const std::string& to_dialect);

private:
    std::unique_ptr<amphi::proto::AmphiService::Stub> stub_;
};

#endif //PIXELS_WORKER_AMPHI_TRANSPILE_SQL_CLIENT_H
