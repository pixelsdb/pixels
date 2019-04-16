package cn.edu.ruc.iir.pixels.daemon.metadata;

import cn.edu.ruc.iir.pixels.daemon.MetadataProto;
import cn.edu.ruc.iir.pixels.daemon.MetadataServiceGrpc;
import cn.edu.ruc.iir.pixels.daemon.metadata.dao.ColumnDao;
import cn.edu.ruc.iir.pixels.daemon.metadata.dao.PbLayoutDao;
import cn.edu.ruc.iir.pixels.daemon.metadata.dao.PbSchemaDao;
import cn.edu.ruc.iir.pixels.daemon.metadata.dao.PbTableDao;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * Created at: 19-4-16
 * Author: hank
 */
public class MetadataServiceImpl extends MetadataServiceGrpc.MetadataServiceImplBase
{
    private static Logger log = LogManager.getLogger(MetadataServiceImpl.class);

    private PbSchemaDao schemaDao = new PbSchemaDao();
    private PbTableDao tableDao = new PbTableDao();
    private PbLayoutDao layoutDao = new PbLayoutDao();
    private ColumnDao columnDao = new ColumnDao();

    public MetadataServiceImpl () { }

    /**
     * @param request
     * @param responseObserver
     */
    @Override
    public void getSchemas(MetadataProto.GetSchemasRequest request, StreamObserver<MetadataProto.GetSchemasResponse> responseObserver)
    {
        MetadataProto.ResponseHeader.Builder headerBuilder = MetadataProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());
        List<MetadataProto.Schema> schemas = this.schemaDao.getAll();
        MetadataProto.ResponseHeader header = headerBuilder
                .setErrorCode(0)
                .setErrorMsg("").build();
        MetadataProto.GetSchemasResponse response = MetadataProto.GetSchemasResponse.newBuilder()
                .setHeader(header)
                .addAllSchemas(schemas).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * @param request
     * @param responseObserver
     */
    @Override
    public void getTables(MetadataProto.GetTablesRequest request, StreamObserver<MetadataProto.GetTablesResponse> responseObserver)
    {
        MetadataProto.ResponseHeader.Builder headerBuilder = MetadataProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());
        MetadataProto.ResponseHeader header;
        MetadataProto.GetTablesResponse response;
        MetadataProto.Schema schema = schemaDao.getByName(request.getSchemaName());
        List<MetadataProto.Table> tables;

        if(schema != null)
        {
            tables = tableDao.getBySchema(schema);
            header = headerBuilder.setErrorCode(0).setErrorMsg("").build();
            response = MetadataProto.GetTablesResponse.newBuilder()
                    .setHeader(header)
                    .addAllTables(tables).build();
        }
        else
        {
            // TODO: configure and use error code in common.
            header = headerBuilder.setErrorCode(1).setErrorMsg("schema not exists").build();
            response = MetadataProto.GetTablesResponse.newBuilder().setHeader(header).build();
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * @param request
     * @param responseObserver
     */
    @Override
    public void getLayouts(MetadataProto.GetLayoutsRequest request, StreamObserver<MetadataProto.GetLayoutsResponse> responseObserver)
    {
        MetadataProto.ResponseHeader.Builder headerBuilder = MetadataProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());
        MetadataProto.GetLayoutsResponse response;
        MetadataProto.Schema schema = schemaDao.getByName(request.getSchemaName());
        List<MetadataProto.Layout> layouts = null;
        if(schema != null)
        {
            MetadataProto.Table table = tableDao.getByNameAndSchema(request.getTableName(), schema);
            if (table != null)
            {
                layouts = layoutDao.getReadableByTable(table, null);
                if (layouts == null || layouts.isEmpty())
                {
                    headerBuilder.setErrorCode(1).setErrorMsg("layout not exist");
                }
            }
            else
            {
                headerBuilder.setErrorCode(1).setErrorMsg("table not exist");
            }
        }
        else
        {
            headerBuilder.setErrorCode(1).setErrorMsg("schema not exist");
        }
        if(layouts != null && layouts.isEmpty() == false)
        {
            headerBuilder.setErrorCode(0).setErrorMsg("");
            response = MetadataProto.GetLayoutsResponse.newBuilder()
                    .setHeader(headerBuilder.build())
                    .addAllLayouts(layouts).build();
        }
        else
        {
            response = MetadataProto.GetLayoutsResponse.newBuilder()
                    .setHeader(headerBuilder.build()).build();
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * @param request
     * @param responseObserver
     */
    @Override
    public void getLayout(MetadataProto.GetLayoutRequest request, StreamObserver<MetadataProto.GetLayoutResponse> responseObserver)
    {
        MetadataProto.ResponseHeader.Builder headerBuilder = MetadataProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());
        MetadataProto.GetLayoutResponse response;
        MetadataProto.Schema schema = schemaDao.getByName(request.getSchemaName());
        MetadataProto.Layout layout = null;
        if(schema != null)
        {
            MetadataProto.Table table = tableDao.getByNameAndSchema(request.getTableName(), schema);
            if (table != null)
            {
                List<MetadataProto.Layout> layouts = layoutDao.getReadableByTable(table, request.getVersion()+"");
                if (layouts == null || layouts.isEmpty())
                {
                    headerBuilder.setErrorCode(1).setErrorMsg("layout not exist");
                }
                else if (layouts.size() != 1)
                {
                    headerBuilder.setErrorCode(1).setErrorMsg("duplicate layouts");
                }
                else
                {
                    layout = layouts.get(0);
                }
            }
            else
            {
                headerBuilder.setErrorCode(1).setErrorMsg("table not exist");
            }
        }
        else
        {
            headerBuilder.setErrorCode(1).setErrorMsg("schema not exist");
        }
        if(layout != null)
        {
            headerBuilder.setErrorCode(0).setErrorMsg("");
            response = MetadataProto.GetLayoutResponse.newBuilder()
                    .setHeader(headerBuilder.build())
                    .setLayout(layout).build();
        }
        else
        {
            response = MetadataProto.GetLayoutResponse.newBuilder()
                    .setHeader(headerBuilder.build()).build();
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * @param request
     * @param responseObserver
     */
    @Override
    public void getColumns(MetadataProto.GetColumnsRequest request, StreamObserver<MetadataProto.GetColumnsResponse> responseObserver)
    {
        MetadataProto.ResponseHeader.Builder headerBuilder = MetadataProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());
        super.getColumns(request, responseObserver);
    }

    /**
     * @param request
     * @param responseObserver
     */
    @Override
    public void createSchema(MetadataProto.CreateSchemaRequest request, StreamObserver<MetadataProto.CreateSchemaResponse> responseObserver)
    {
        MetadataProto.ResponseHeader.Builder headerBuilder = MetadataProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());
        super.createSchema(request, responseObserver);
    }

    /**
     * @param request
     * @param responseObserver
     */
    @Override
    public void dropSchema(MetadataProto.DropSchemaRequest request, StreamObserver<MetadataProto.DropSchemaResponse> responseObserver)
    {
        MetadataProto.ResponseHeader.Builder headerBuilder = MetadataProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());
        super.dropSchema(request, responseObserver);
    }

    /**
     * @param request
     * @param responseObserver
     */
    @Override
    public void createTable(MetadataProto.CreateTableRequest request, StreamObserver<MetadataProto.CreateTableResponse> responseObserver)
    {
        MetadataProto.ResponseHeader.Builder headerBuilder = MetadataProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());
        super.createTable(request, responseObserver);
    }

    /**
     * @param request
     * @param responseObserver
     */
    @Override
    public void dropTable(MetadataProto.DropTableRequest request, StreamObserver<MetadataProto.DropTableResponse> responseObserver)
    {
        MetadataProto.ResponseHeader.Builder headerBuilder = MetadataProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());
        super.dropTable(request, responseObserver);
    }

    /**
     * @param request
     * @param responseObserver
     */
    @Override
    public void existTable(MetadataProto.ExistTableRequest request, StreamObserver<MetadataProto.ExistTableResponse> responseObserver)
    {
        MetadataProto.ResponseHeader.Builder headerBuilder = MetadataProto.ResponseHeader.newBuilder()
                .setToken(request.getHeader().getToken());
        super.existTable(request, responseObserver);
    }
}
