package cn.edu.ruc.iir.pixels.daemon.metadata;

import cn.edu.ruc.iir.pixels.daemon.MetadataProto;
import cn.edu.ruc.iir.pixels.daemon.MetadataServiceGrpc;
import cn.edu.ruc.iir.pixels.daemon.metadata.dao.ColumnDao;
import cn.edu.ruc.iir.pixels.daemon.metadata.dao.LayoutDao;
import cn.edu.ruc.iir.pixels.daemon.metadata.dao.SchemaDao;
import cn.edu.ruc.iir.pixels.daemon.metadata.dao.TableDao;
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

    private SchemaDao schemaDao = new SchemaDao();
    private TableDao tableDao = new TableDao();
    private LayoutDao layoutDao = new LayoutDao();
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
                layouts = layoutDao.getReadableByTable(table, -1); // version < 0 means get all versions
                if (layouts == null || layouts.isEmpty())
                {
                    headerBuilder.setErrorCode(1).setErrorMsg("no layouts found");
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
                List<MetadataProto.Layout> layouts = layoutDao.getReadableByTable(table, request.getVersion());
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
        MetadataProto.GetColumnsResponse response;
        MetadataProto.Schema schema = schemaDao.getByName(request.getSchemaName());
        List<MetadataProto.Column> columns = null;
        if(schema != null) {
            MetadataProto.Table table = tableDao.getByNameAndSchema(request.getTableName(), schema);
            if (table != null) {
                columns = columnDao.getByTable(table);
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
        if(columns != null && columns.isEmpty() == false)
        {
            headerBuilder.setErrorCode(0).setErrorMsg("");
            response = MetadataProto.GetColumnsResponse.newBuilder()
                    .setHeader(headerBuilder.build())
                    .addAllColumns(columns).build();
        }
        else
        {
            headerBuilder.setErrorCode(1).setErrorMsg("no columns found");
            response = MetadataProto.GetColumnsResponse.newBuilder()
                    .setHeader(headerBuilder.build())
                    .addAllColumns(columns).build();
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
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

        MetadataProto.Schema schema= MetadataProto.Schema.newBuilder()
        .setName(request.getSchemaName())
        .setDesc(request.getSchemaDesc()).build();
        if (schemaDao.exists(schema))
        {
            headerBuilder.setErrorCode(1).setErrorMsg("exists");
        }
        else
        {
            schemaDao.insert(schema);
            headerBuilder.setErrorCode(0).setErrorMsg("exists");
        }
        MetadataProto.CreateSchemaResponse response = MetadataProto.CreateSchemaResponse.newBuilder()
                .setHeader(headerBuilder.build()).build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
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

        if (schemaDao.deleteByName(request.getSchemaName()))
        {
            headerBuilder.setErrorCode(0).setErrorMsg("");
        }
        else
        {
            headerBuilder.setErrorCode(1).setErrorMsg("failed to drop schema");
        }
        MetadataProto.DropSchemaResponse response = MetadataProto.DropSchemaResponse.newBuilder()
                .setHeader(headerBuilder.build()).build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
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

        MetadataProto.Schema schema = schemaDao.getByName(request.getSchemaName());
        MetadataProto.Table table = MetadataProto.Table.newBuilder()
        .setName(request.getTableName())
        .setType("user")
        .setSchemaId(schema.getId()).build();
        if (tableDao.exists(table))
        {
            headerBuilder.setErrorCode(1).setErrorMsg("table already exist");
        }
        else
        {
            tableDao.insert(table);
            List<MetadataProto.Column> columns = request.getColumnsList();
            // to get table id from database.
            table = tableDao.getByNameAndSchema(table.getName(), schema);
            if (columns.size() == columnDao.insertBatch(table, columns))
            {
                headerBuilder.setErrorCode(0).setErrorMsg("");
            }
            else
            {
                tableDao.deleteByNameAndSchema(table.getName(), schema);
                headerBuilder.setErrorCode(1).setErrorMsg("failed to create table");
            }
        }

        MetadataProto.CreateTableResponse response = MetadataProto.CreateTableResponse.newBuilder()
                .setHeader(headerBuilder.build()).build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
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

        MetadataProto.Schema schema = schemaDao.getByName(request.getSchemaName());
        if (tableDao.deleteByNameAndSchema(request.getTableName(), schema))
        {
            headerBuilder.setErrorCode(0).setErrorMsg("");
        }
        else
        {
            headerBuilder.setErrorCode(1).setErrorMsg("failed to drop table");
        }
        MetadataProto.DropTableResponse response = MetadataProto.DropTableResponse.newBuilder()
                .setHeader(headerBuilder.build()).build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
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

        MetadataProto.Schema schema = schemaDao.getByName(request.getSchemaName());
        MetadataProto.Table table = MetadataProto.Table.newBuilder()
        .setId(-1)
        .setName(request.getTableName())
        .setSchemaId(schema.getId()).build();
        MetadataProto.ExistTableResponse response;
        if (tableDao.exists(table))
        {
            headerBuilder.setErrorCode(0).setErrorMsg("");
            response = MetadataProto.ExistTableResponse.newBuilder()
                    .setExists(true).setHeader(headerBuilder.build()).build();
        }
        else
        {
            headerBuilder.setErrorCode(0).setErrorMsg("");
            response = MetadataProto.ExistTableResponse.newBuilder()
                    .setExists(false).setHeader(headerBuilder.build()).build();
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
