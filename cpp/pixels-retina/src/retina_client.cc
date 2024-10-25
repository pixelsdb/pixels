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
#include <grpcpp/grpcpp.h>

#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>

#include "retina.grpc.pb.h"

/**
 * @author mzp0514
 * @date 27/05/2022
 */

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using retina::proto::DeleteRecordRequest;
using retina::proto::DeleteRecordResponse;
using retina::proto::InsertRecordRequest;
using retina::proto::InsertRecordResponse;
using retina::proto::RequestHeader;
using retina::proto::ResponseHeader;
using retina::proto::RetinaService;
using retina::proto::UpdateRecordRequest;
using retina::proto::UpdateRecordResponse;
using retina::proto::Value;

class RetinaClient {
 public:
  RetinaClient(std::shared_ptr<Channel> channel)
      : stub_(RetinaService::NewStub(channel)) {}

  // Assembles the client's payload, sends it and presents the response back
  // from the server.
  std::string Insert(std::string& schema_name, std::string& table_name,
                     int pkid, std::vector<Value>& values, long ts) {
    // Data we are sending to the server.
    InsertRecordRequest request;
    request.set_schemaname(schema_name);
    request.set_tablename(table_name);
    request.set_timestamp(ts);
    request.set_pkid(pkid);
    for (int i = 0; i < values.size(); i++) {
      auto value = request.add_values();
      value->Swap(&values[i]);
    }

    // Container for the data we expect from the server.
    InsertRecordResponse reply;

    // Context for the client. It could be used to convey extra information to
    // the server and/or tweak certain RPC behaviors.
    ClientContext context;

    // The actual RPC.
    Status status = stub_->InsertRecord(&context, request, &reply);

    // Act upon its status.
    if (status.ok()) {
      return "ok";
    } else {
      std::cout << status.error_code() << ": " << status.error_message()
                << std::endl;
      return "RPC failed";
    }
  }

 private:
  std::unique_ptr<RetinaService::Stub> stub_;
};

std::string makeFixedLength(const int i, const int length) {
  std::ostringstream ostr;

  if (i < 0) ostr << '-';

  ostr << std::setfill('0') << std::setw(length) << (i < 0 ? -i : i);

  return ostr.str();
}

int main(int argc, char** argv) {
  // Instantiate the client. It requires a channel, out of which the actual RPCs
  // are created. This channel models a connection to an endpoint specified by
  // the argument "--target=" which is the only expected argument.
  // We indicate that the channel isn't authenticated (use of
  // InsecureChannelCredentials()).
  std::string target_str;
  std::string arg_str("--target");
  if (argc > 1) {
    std::string arg_val = argv[1];
    size_t start_pos = arg_val.find(arg_str);
    if (start_pos != std::string::npos) {
      start_pos += arg_str.size();
      if (arg_val[start_pos] == '=') {
        target_str = arg_val.substr(start_pos + 1);
      } else {
        std::cout << "The only correct argument syntax is --target="
                  << std::endl;
        return 0;
      }
    } else {
      std::cout << "The only acceptable argument is --target=" << std::endl;
      return 0;
    }
  } else {
    target_str = "localhost:50051";
  }
  RetinaClient client(
      grpc::CreateChannel(target_str, grpc::InsecureChannelCredentials()));

  const int row_num = 791;

  long long_vals[4][row_num][5];
  std::string str_vals[4][row_num][2];
  long ver_vals[4][row_num];
  int indices[4][row_num];
  int pk = 6;
  std::string schema_name = "s0";
  std::string table_name = "t0";

  int u = 0;
  for (int k = 0; k < 4; k++) {
    for (int i = 0; i < row_num; i++) {
      indices[k][i] = i;
      for (int j = 0; j < 2; j++) {
        str_vals[k][i][j] = makeFixedLength(u, 4);
      }
      for (int j = 0; j < 5; j++) {
        long_vals[k][i][j] = u;
      }
      ver_vals[k][i] = u;
      u++;
    }

    std::sort(indices[k], indices[k] + row_num, [&](int i, int j) {
      return str_vals[k][i][0] < str_vals[k][j][0];
    });
  }

  for (int k = 0; k < 1; k++) {
    auto long_vals_k = long_vals[k];
    auto str_vals_k = str_vals[k];
    auto ver_vals_k = ver_vals[k];

    for (int i = 0; i < row_num; i++) {
      std::vector<Value> values;
      for (int j = 0; j < 5; j++) {
        Value val;
        val.set_longvalue(long_vals_k[i][j]);
        values.push_back(val);
      }

      for (int j = 0; j < 2; j++) {
        Value val;
        val.set_stringvalue(str_vals_k[i][j]);
        values.push_back(val);
      }

      std::string reply =
          client.Insert(schema_name, table_name, pk, values, ver_vals_k[i]);
      std::cout << "Insert response: " << reply << std::endl;
    }
  }

  return 0;
}
