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
#ifndef PIXELS_AMPHI_WORKER_METADATA_CLIENT_H
#define PIXELS_AMPHI_WORKER_METADATA_CLIENT_H

#include <iostream>
#include <memory>
#include <string>

#include <grpc++/grpc++.h>
#include "metadata.pb.h"
#include "metadata.grpc.pb.h"

class MetadataClient {
public:
    MetadataClient(std::shared_ptr<grpc::Channel> channel);

    // RPC services
    metadata::proto::CreateSchemaResponse CreateSchema(const metadata::proto::CreateSchemaRequest& request);
    metadata::proto::ExistSchemaResponse ExistSchema(const metadata::proto::ExistSchemaRequest& request);
    metadata::proto::GetSchemasResponse GetSchemas(const metadata::proto::GetSchemasRequest& request);
    metadata::proto::DropSchemaResponse DropSchema(const metadata::proto::DropSchemaRequest& request);
    metadata::proto::CreateTableResponse CreateTable(const metadata::proto::CreateTableRequest& request);
    metadata::proto::ExistTableResponse ExistTable(const metadata::proto::ExistTableRequest& request);
    metadata::proto::GetTableResponse GetTable(const metadata::proto::GetTableRequest& request);
    metadata::proto::GetTablesResponse GetTables(const metadata::proto::GetTablesRequest& request);
    metadata::proto::UpdateRowCountResponse UpdateRowCount(const metadata::proto::UpdateRowCountRequest& request);
    metadata::proto::DropTableResponse DropTable(const metadata::proto::DropTableRequest& request);
    metadata::proto::AddLayoutResponse AddLayout(const metadata::proto::AddLayoutRequest& request);
    metadata::proto::GetLayoutsResponse GetLayouts(const metadata::proto::GetLayoutsRequest& request);
    metadata::proto::GetLayoutResponse GetLayout(const metadata::proto::GetLayoutRequest& request);
    metadata::proto::UpdateLayoutResponse UpdateLayout(const metadata::proto::UpdateLayoutRequest& request);
    metadata::proto::CreatePathResponse CreatePath(const metadata::proto::CreatePathRequest& request);
    metadata::proto::GetPathsResponse GetPaths(const metadata::proto::GetPathsRequest& request);
    metadata::proto::UpdatePathResponse UpdatePath(const metadata::proto::UpdatePathRequest& request);
    metadata::proto::DeletePathsResponse DeletePaths(const metadata::proto::DeletePathsRequest& request);
    metadata::proto::CreatePeerPathResponse CreatePeerPath(const metadata::proto::CreatePeerPathRequest& request);
    metadata::proto::GetPeerPathsResponse GetPeerPaths(const metadata::proto::GetPeerPathsRequest& request);
    metadata::proto::UpdatePeerPathResponse UpdatePeerPath(const metadata::proto::UpdatePeerPathRequest& request);
    metadata::proto::DeletePeerPathsResponse DeletePeerPaths(const metadata::proto::DeletePeerPathsRequest& request);
    metadata::proto::CreatePeerResponse CreatePeer(const metadata::proto::CreatePeerRequest& request);
    metadata::proto::GetPeerResponse GetPeer(const metadata::proto::GetPeerRequest& request);
    metadata::proto::UpdatePeerResponse UpdatePeer(const metadata::proto::UpdatePeerRequest& request);
    metadata::proto::DeletePeerResponse DeletePeer(const metadata::proto::DeletePeerRequest& request);
    metadata::proto::GetColumnsResponse GetColumns(const metadata::proto::GetColumnsRequest& request);
    metadata::proto::UpdateColumnResponse UpdateColumn(const metadata::proto::UpdateColumnRequest& request);
    metadata::proto::CreateViewResponse CreateView(const metadata::proto::CreateViewRequest& request);
    metadata::proto::ExistViewResponse ExistView(const metadata::proto::ExistViewRequest& request);
    metadata::proto::GetViewsResponse GetViews(const metadata::proto::GetViewsRequest& request);
    metadata::proto::GetViewResponse GetView(const metadata::proto::GetViewRequest& request);
    metadata::proto::DropViewResponse DropView(const metadata::proto::DropViewRequest& request);

private:
    std::unique_ptr<metadata::proto::MetadataService::Stub> stub_;
};

#endif //PIXELS_AMPHI_WORKER_METADATA_CLIENT_H
