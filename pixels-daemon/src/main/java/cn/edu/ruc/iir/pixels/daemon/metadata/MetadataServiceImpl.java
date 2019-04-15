package cn.edu.ruc.iir.pixels.daemon.metadata;

import cn.edu.ruc.iir.pixels.daemon.MetadataProto;
import cn.edu.ruc.iir.pixels.daemon.MetadataServiceGrpc;
import io.grpc.stub.StreamObserver;

/**
 * Created at: 19-4-16
 * Author: hank
 */
public class MetadataServiceImpl extends MetadataServiceGrpc.MetadataServiceImplBase
{
    /**
     * @param request
     * @param responseObserver
     */
    @Override
    public void getSchemas(MetadataProto.GetSchemasRequest request, StreamObserver<MetadataProto.GetSchemasResponse> responseObserver)
    {
        super.getSchemas(request, responseObserver);
    }

    /**
     * @param request
     * @param responseObserver
     */
    @Override
    public void getTables(MetadataProto.GetTablesRequest request, StreamObserver<MetadataProto.GetTablesResponse> responseObserver)
    {
        super.getTables(request, responseObserver);
    }

    /**
     * @param request
     * @param responseObserver
     */
    @Override
    public void getLayouts(MetadataProto.GetLayoutsRequest request, StreamObserver<MetadataProto.GetLayoutsResponse> responseObserver)
    {
        super.getLayouts(request, responseObserver);
    }

    /**
     * @param request
     * @param responseObserver
     */
    @Override
    public void getLayout(MetadataProto.GetLayoutRequest request, StreamObserver<MetadataProto.GetLayoutResponse> responseObserver)
    {
        super.getLayout(request, responseObserver);
    }

    /**
     * @param request
     * @param responseObserver
     */
    @Override
    public void getColumns(MetadataProto.GetColumnsRequest request, StreamObserver<MetadataProto.GetColumnsResponse> responseObserver)
    {
        super.getColumns(request, responseObserver);
    }

    /**
     * @param request
     * @param responseObserver
     */
    @Override
    public void createSchema(MetadataProto.CreateSchemaRequest request, StreamObserver<MetadataProto.CreateSchemaResponse> responseObserver)
    {
        super.createSchema(request, responseObserver);
    }

    /**
     * @param request
     * @param responseObserver
     */
    @Override
    public void dropSchema(MetadataProto.DropSchemaRequest request, StreamObserver<MetadataProto.DropSchemaResponse> responseObserver)
    {
        super.dropSchema(request, responseObserver);
    }

    /**
     * @param request
     * @param responseObserver
     */
    @Override
    public void createTable(MetadataProto.CreateTableRequest request, StreamObserver<MetadataProto.CreateTableResponse> responseObserver)
    {
        super.createTable(request, responseObserver);
    }

    /**
     * @param request
     * @param responseObserver
     */
    @Override
    public void dropTable(MetadataProto.DropTableRequest request, StreamObserver<MetadataProto.DropTableResponse> responseObserver)
    {
        super.dropTable(request, responseObserver);
    }

    /**
     * @param request
     * @param responseObserver
     */
    @Override
    public void existTable(MetadataProto.ExistTableRequest request, StreamObserver<MetadataProto.ExistTableResponse> responseObserver)
    {
        super.existTable(request, responseObserver);
    }
}
