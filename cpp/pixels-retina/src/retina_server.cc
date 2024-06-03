/*
 * Copyright 2017-2019 PixelsDB.
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
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

#include "retina_service.grpc.pb.h"
#include "retina_service_impl.h"

/**
 * @author mzp0514
 * @date 27/05/2022
 */

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
using retina::DeleteRecordRequest;
using retina::DeleteRecordResponse;
using retina::InsertRecordRequest;
using retina::InsertRecordResponse;
using retina::QueryAck;
using retina::QueryRecordsRequest;
using retina::QueryRecordsResponse;
using retina::QueryVisibilityRequest;
using retina::QueryVisibilityResponse;
using retina::RequestHeader;
using retina::ResponseHeader;
using retina::RetinaService;
using retina::UpdateRecordRequest;
using retina::UpdateRecordResponse;
using retina::Value;

using std::chrono::system_clock;

log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("RetinaServer"));

class RetinaServer final : public RetinaService::Service {
 public:
  Status UpdateRecord(ServerContext* context,
                      const UpdateRecordRequest* request,
                      UpdateRecordResponse* response) {
    std::vector<Slice> values;
    char buf[1024];
    int pos = 0;
    for (int i = 0; i < request->originalvalues_size(); i++) {
      Value value = request->originalvalues(i);
      if (value.has_longvalue()) {
        int64_t long_value = value.longvalue();
        memcpy(buf + pos, (char*)&long_value, sizeof(int64_t));
        values.push_back(Slice(buf + pos, sizeof(int64_t)));
        pos += sizeof(int64_t);
      } else if (value.has_stringvalue()) {
        std::string* string_value = value.mutable_stringvalue();
        memcpy(buf + pos, string_value->data(), string_value->size());
        values.push_back(Slice(buf + pos, string_value->size()));
        pos += string_value->size();
      }
    }
    service_.Update(request->schemaname(), request->tablename(),
                    request->pkid(), values, request->timestamp());
    return Status::OK;
  }

  Status InsertRecord(ServerContext* context,
                      const InsertRecordRequest* request,
                      InsertRecordResponse* response) {
    std::vector<Slice> values;
    std::vector<ValueType> types;
    char buf[1024];
    int pos = 0;
    for (int i = 0; i < request->values_size(); i++) {
      Value value = request->values(i);
      if (value.has_longvalue()) {
        int64_t long_value = value.longvalue();
        memcpy(buf + pos, (char*)&long_value, sizeof(int64_t));
        values.push_back(Slice(buf + pos, sizeof(int64_t)));
        pos += sizeof(int64_t);
        types.push_back(ValueType::LONG);
      } else if (value.has_stringvalue()) {
        std::string* string_value = value.mutable_stringvalue();
        memcpy(buf + pos, string_value->data(), string_value->size());
        values.push_back(Slice(buf + pos, string_value->size()));
        pos += string_value->size();
        types.push_back(ValueType::BYTES);
      }
    }

    service_.Insert(request->schemaname(), request->tablename(),
                    request->pkid(), values, types, request->timestamp());
    return Status::OK;
  }

  Status DeleteRecord(ServerContext* context,
                      const DeleteRecordRequest* request,
                      DeleteRecordResponse* response) {
    Slice pk;
    char buf[1024];
    Value value = request->primarykey();
    if (value.has_longvalue()) {
      int64_t long_value = value.longvalue();
      memcpy(buf, (char*)&long_value, sizeof(int64_t));
      pk = Slice(buf, sizeof(int64_t));
    } else if (value.has_stringvalue()) {
      std::string* string_value = value.mutable_stringvalue();
      memcpy(buf, string_value->data(), string_value->size());
      pk = Slice(Slice(buf, string_value->size()));
    }
    service_.Delete(request->schemaname(), request->tablename(),
                    request->pkid(), pk, request->timestamp());
    return Status::OK;
  }

  Status QueryRecords(ServerContext* context,
                      const QueryRecordsRequest* request,
                      QueryRecordsResponse* response) {
    long pos = 0;
    service_.Query(request->schemaname(), request->tablename(), request->rgid(),
                   request->timestamp(), pos);
    response->set_pos(pos);
    return Status::OK;
  }

  Status QueryVisibility(ServerContext* context,
                         const QueryVisibilityRequest* request,
                         QueryVisibilityResponse* response) {
    long pos = 0;
    service_.QueryVisibility(request->schemaname(), request->tablename(),
                             request->rgid(), request->timestamp(), pos);
    response->set_pos(pos);
    return Status::OK;
  }

  Status FinishRecords(ServerContext* context, const QueryAck* request,
                       ResponseHeader* response) {
    service_.ReleaseQueryMem(request->pos());
    return Status::OK;
  }

  Status FinishVisibility(ServerContext* context, const QueryAck* request,
                          ResponseHeader* response) {
    service_.ReleaseVersionMem(request->pos());
    return Status::OK;
  }

 private:
  RetinaServiceImpl service_;
};

void RunServer() {
  std::string server_address("0.0.0.0:50051");
  log4cxx::BasicConfigurator::configure();
  RetinaServer service;

  grpc::EnableDefaultHealthCheckService(true);
  grpc::reflection::InitProtoReflectionServerBuilderPlugin();
  ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&service);
  // Finally assemble the server.
  std::unique_ptr<Server> server(builder.BuildAndStart());
  LOG4CXX_DEBUG_FMT(logger, "Server listening on {}", server_address);
  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}

int main(int argc, char** argv) {
  RunServer();

  return 0;
}