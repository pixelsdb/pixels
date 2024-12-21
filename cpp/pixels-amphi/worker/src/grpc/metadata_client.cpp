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
#include "grpc/metadata_client.h"

#include "exception/grpc_exception.h"
#include "exception/grpc_metadata_exception.h"

MetadataClient::MetadataClient(std::shared_ptr <grpc::Channel> channel)
        : stub_(metadata::proto::MetadataService::NewStub(channel))
{};

metadata::proto::CreateSchemaResponse MetadataClient::CreateSchema(
        const metadata::proto::CreateSchemaRequest &request)
{
    metadata::proto::CreateSchemaResponse response;
    grpc::ClientContext context;

    grpc::Status status = stub_->CreateSchema(&context, request, &response);

    if (!status.ok())
    {
        throw GrpcMetadataException(response.header().errormsg());
    }

    return response;
}

metadata::proto::ExistSchemaResponse MetadataClient::ExistSchema(
        const metadata::proto::ExistSchemaRequest &request)
{
    metadata::proto::ExistSchemaResponse response;
    grpc::ClientContext context;

    grpc::Status status = stub_->ExistSchema(&context, request, &response);

    if (!status.ok())
    {
        throw GrpcMetadataException(response.header().errormsg());
    }

    return response;
}

metadata::proto::GetSchemasResponse MetadataClient::GetSchemas(
        const metadata::proto::GetSchemasRequest &request)
{
    metadata::proto::GetSchemasResponse response;
    grpc::ClientContext context;

    grpc::Status status = stub_->GetSchemas(&context, request, &response);

    if (!status.ok())
    {
        throw GrpcMetadataException(response.header().errormsg());
    }

    return response;
}

metadata::proto::DropSchemaResponse MetadataClient::DropSchema(
        const metadata::proto::DropSchemaRequest &request)
{
    metadata::proto::DropSchemaResponse response;
    grpc::ClientContext context;

    grpc::Status status = stub_->DropSchema(&context, request, &response);

    if (!status.ok())
    {
        throw GrpcMetadataException(response.header().errormsg());
    }

    return response;
}

metadata::proto::CreateTableResponse MetadataClient::CreateTable(
        const metadata::proto::CreateTableRequest &request)
{
    metadata::proto::CreateTableResponse response;
    grpc::ClientContext context;

    grpc::Status status = stub_->CreateTable(&context, request, &response);

    if (!status.ok())
    {
        throw GrpcMetadataException(response.header().errormsg());
    }

    return response;
}

metadata::proto::ExistTableResponse MetadataClient::ExistTable(
        const metadata::proto::ExistTableRequest &request)
{
    metadata::proto::ExistTableResponse response;
    grpc::ClientContext context;

    grpc::Status status = stub_->ExistTable(&context, request, &response);

    if (!status.ok())
    {
        throw GrpcMetadataException(response.header().errormsg());
    }

    return response;
}

metadata::proto::GetTableResponse MetadataClient::GetTable(
        const metadata::proto::GetTableRequest &request)
{
    metadata::proto::GetTableResponse response;
    grpc::ClientContext context;

    grpc::Status status = stub_->GetTable(&context, request, &response);

    if (!status.ok())
    {
        throw GrpcMetadataException(response.header().errormsg());
    }

    return response;
}

metadata::proto::GetTablesResponse MetadataClient::GetTables(
        const metadata::proto::GetTablesRequest &request)
{
    metadata::proto::GetTablesResponse response;
    grpc::ClientContext context;

    grpc::Status status = stub_->GetTables(&context, request, &response);

    if (!status.ok())
    {
        throw GrpcMetadataException(response.header().errormsg());
    }

    return response;
}

metadata::proto::UpdateRowCountResponse MetadataClient::UpdateRowCount(
        const metadata::proto::UpdateRowCountRequest &request)
{
    metadata::proto::UpdateRowCountResponse response;
    grpc::ClientContext context;

    grpc::Status status = stub_->UpdateRowCount(&context, request, &response);

    if (!status.ok())
    {
        throw GrpcMetadataException(response.header().errormsg());
    }

    return response;
}

metadata::proto::DropTableResponse MetadataClient::DropTable(
        const metadata::proto::DropTableRequest &request)
{
    metadata::proto::DropTableResponse response;
    grpc::ClientContext context;

    grpc::Status status = stub_->DropTable(&context, request, &response);

    if (!status.ok())
    {
        throw GrpcMetadataException(response.header().errormsg());
    }

    return response;
}

metadata::proto::AddLayoutResponse MetadataClient::AddLayout(
        const metadata::proto::AddLayoutRequest &request)
{
    metadata::proto::AddLayoutResponse response;
    grpc::ClientContext context;

    grpc::Status status = stub_->AddLayout(&context, request, &response);

    if (!status.ok())
    {
        throw GrpcMetadataException(response.header().errormsg());
    }

    return response;
}

metadata::proto::GetLayoutsResponse MetadataClient::GetLayouts(
        const metadata::proto::GetLayoutsRequest &request)
{
    metadata::proto::GetLayoutsResponse response;
    grpc::ClientContext context;

    grpc::Status status = stub_->GetLayouts(&context, request, &response);

    if (!status.ok())
    {
        throw GrpcMetadataException(response.header().errormsg());
    }

    return response;
}

metadata::proto::GetLayoutResponse MetadataClient::GetLayout(
        const metadata::proto::GetLayoutRequest &request)
{
    metadata::proto::GetLayoutResponse response;
    grpc::ClientContext context;

    grpc::Status status = stub_->GetLayout(&context, request, &response);

    if (!status.ok())
    {
        throw GrpcMetadataException(response.header().errormsg());
    }

    return response;
}

metadata::proto::UpdateLayoutResponse MetadataClient::UpdateLayout(
        const metadata::proto::UpdateLayoutRequest &request)
{
    metadata::proto::UpdateLayoutResponse response;
    grpc::ClientContext context;

    grpc::Status status = stub_->UpdateLayout(&context, request, &response);

    if (!status.ok())
    {
        throw GrpcMetadataException(response.header().errormsg());
    }

    return response;
}

metadata::proto::CreatePathResponse MetadataClient::CreatePath(
        const metadata::proto::CreatePathRequest &request)
{
    metadata::proto::CreatePathResponse response;
    grpc::ClientContext context;

    grpc::Status status = stub_->CreatePath(&context, request, &response);

    if (!status.ok())
    {
        throw GrpcMetadataException(response.header().errormsg());
    }

    return response;
}

metadata::proto::GetPathsResponse MetadataClient::GetPaths(
        const metadata::proto::GetPathsRequest &request)
{
    metadata::proto::GetPathsResponse response;
    grpc::ClientContext context;

    grpc::Status status = stub_->GetPaths(&context, request, &response);

    if (!status.ok())
    {
        throw GrpcMetadataException(response.header().errormsg());
    }

    return response;
}

metadata::proto::UpdatePathResponse MetadataClient::UpdatePath(
        const metadata::proto::UpdatePathRequest &request)
{
    metadata::proto::UpdatePathResponse response;
    grpc::ClientContext context;

    grpc::Status status = stub_->UpdatePath(&context, request, &response);

    if (!status.ok())
    {
        throw GrpcMetadataException(response.header().errormsg());
    }

    return response;
}

metadata::proto::DeletePathsResponse MetadataClient::DeletePaths(
        const metadata::proto::DeletePathsRequest &request)
{
    metadata::proto::DeletePathsResponse response;
    grpc::ClientContext context;

    grpc::Status status = stub_->DeletePaths(&context, request, &response);

    if (!status.ok())
    {
        throw GrpcMetadataException(response.header().errormsg());
    }

    return response;
}

metadata::proto::CreatePeerPathResponse MetadataClient::CreatePeerPath(
        const metadata::proto::CreatePeerPathRequest &request)
{
    metadata::proto::CreatePeerPathResponse response;
    grpc::ClientContext context;

    grpc::Status status = stub_->CreatePeerPath(&context, request, &response);

    if (!status.ok())
    {
        throw GrpcMetadataException(response.header().errormsg());
    }

    return response;
}

metadata::proto::GetPeerPathsResponse MetadataClient::GetPeerPaths(
        const metadata::proto::GetPeerPathsRequest &request)
{
    metadata::proto::GetPeerPathsResponse response;
    grpc::ClientContext context;

    grpc::Status status = stub_->GetPeerPaths(&context, request, &response);

    if (!status.ok())
    {
        throw GrpcMetadataException(response.header().errormsg());
    }

    return response;
}

metadata::proto::UpdatePeerPathResponse MetadataClient::UpdatePeerPath(
        const metadata::proto::UpdatePeerPathRequest &request)
{
    metadata::proto::UpdatePeerPathResponse response;
    grpc::ClientContext context;

    grpc::Status status = stub_->UpdatePeerPath(&context, request, &response);

    if (!status.ok())
    {
        throw GrpcMetadataException(response.header().errormsg());
    }

    return response;
}

metadata::proto::DeletePeerPathsResponse MetadataClient::DeletePeerPaths(
        const metadata::proto::DeletePeerPathsRequest &request)
{
    metadata::proto::DeletePeerPathsResponse response;
    grpc::ClientContext context;

    grpc::Status status = stub_->DeletePeerPaths(&context, request, &response);

    if (!status.ok())
    {
        throw GrpcMetadataException(response.header().errormsg());
    }

    return response;
}

metadata::proto::CreatePeerResponse MetadataClient::CreatePeer(
        const metadata::proto::CreatePeerRequest &request)
{
    metadata::proto::CreatePeerResponse response;
    grpc::ClientContext context;

    grpc::Status status = stub_->CreatePeer(&context, request, &response);

    if (!status.ok())
    {
        throw GrpcMetadataException(response.header().errormsg());
    }

    return response;
}

metadata::proto::GetPeerResponse MetadataClient::GetPeer(
        const metadata::proto::GetPeerRequest &request)
{
    metadata::proto::GetPeerResponse response;
    grpc::ClientContext context;

    grpc::Status status = stub_->GetPeer(&context, request, &response);

    if (!status.ok())
    {
        throw GrpcMetadataException(response.header().errormsg());
    }

    return response;
}

metadata::proto::UpdatePeerResponse MetadataClient::UpdatePeer(
        const metadata::proto::UpdatePeerRequest &request)
{
    metadata::proto::UpdatePeerResponse response;
    grpc::ClientContext context;

    grpc::Status status = stub_->UpdatePeer(&context, request, &response);

    if (!status.ok())
    {
        throw GrpcMetadataException(response.header().errormsg());
    }

    return response;
}

metadata::proto::DeletePeerResponse MetadataClient::DeletePeer(
        const metadata::proto::DeletePeerRequest &request)
{
    metadata::proto::DeletePeerResponse response;
    grpc::ClientContext context;

    grpc::Status status = stub_->DeletePeer(&context, request, &response);

    if (!status.ok())
    {
        throw GrpcMetadataException(response.header().errormsg());
    }

    return response;
}

metadata::proto::GetColumnsResponse MetadataClient::GetColumns(
        const metadata::proto::GetColumnsRequest &request)
{
    metadata::proto::GetColumnsResponse response;
    grpc::ClientContext context;

    grpc::Status status = stub_->GetColumns(&context, request, &response);

    if (!status.ok())
    {
        throw GrpcMetadataException(response.header().errormsg());
    }

    return response;
}

metadata::proto::UpdateColumnResponse MetadataClient::UpdateColumn(
        const metadata::proto::UpdateColumnRequest &request)
{
    metadata::proto::UpdateColumnResponse response;
    grpc::ClientContext context;

    grpc::Status status = stub_->UpdateColumn(&context, request, &response);

    if (!status.ok())
    {
        throw GrpcMetadataException(response.header().errormsg());
    }

    return response;
}

metadata::proto::CreateViewResponse MetadataClient::CreateView(
        const metadata::proto::CreateViewRequest &request)
{
    metadata::proto::CreateViewResponse response;
    grpc::ClientContext context;

    grpc::Status status = stub_->CreateView(&context, request, &response);

    if (!status.ok())
    {
        throw GrpcMetadataException(response.header().errormsg());
    }

    return response;
}

metadata::proto::ExistViewResponse MetadataClient::ExistView(
        const metadata::proto::ExistViewRequest &request)
{
    metadata::proto::ExistViewResponse response;
    grpc::ClientContext context;

    grpc::Status status = stub_->ExistView(&context, request, &response);

    if (!status.ok())
    {
        throw GrpcMetadataException(response.header().errormsg());
    }

    return response;
}

metadata::proto::GetViewsResponse MetadataClient::GetViews(
        const metadata::proto::GetViewsRequest &request)
{
    metadata::proto::GetViewsResponse response;
    grpc::ClientContext context;

    grpc::Status status = stub_->GetViews(&context, request, &response);

    if (!status.ok())
    {
        throw GrpcMetadataException(response.header().errormsg());
    }

    return response;
}

metadata::proto::GetViewResponse MetadataClient::GetView(
        const metadata::proto::GetViewRequest &request)
{
    metadata::proto::GetViewResponse response;
    grpc::ClientContext context;

    grpc::Status status = stub_->GetView(&context, request, &response);

    if (!status.ok())
    {
        throw GrpcMetadataException(response.header().errormsg());
    }

    return response;
}

metadata::proto::DropViewResponse MetadataClient::DropView(
        const metadata::proto::DropViewRequest &request)
{
    metadata::proto::DropViewResponse response;
    grpc::ClientContext context;

    grpc::Status status = stub_->DropView(&context, request, &response);

    if (!status.ok())
    {
        throw GrpcMetadataException(response.header().errormsg());
    }

    return response;
}