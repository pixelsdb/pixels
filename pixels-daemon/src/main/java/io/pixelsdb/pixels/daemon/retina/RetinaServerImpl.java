/*
 * Copyright 2018 PixelsDB.
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
package io.pixelsdb.pixels.daemon.retina;

import io.grpc.stub.StreamObserver;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.*;
import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.physical.PhysicalReaderUtil;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.core.PixelsProto;
import io.pixelsdb.pixels.core.PixelsVersion;
import io.pixelsdb.pixels.core.exception.PixelsFileMagicInvalidException;
import io.pixelsdb.pixels.core.exception.PixelsFileVersionInvalidException;
import io.pixelsdb.pixels.retina.RetinaWorkerServiceGrpc;
import io.pixelsdb.pixels.retina.RetinaProto;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.LinkedList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Created at: 24-12-20
 * Author: gengdy
 */
public class RetinaServerImpl extends RetinaWorkerServiceGrpc.RetinaWorkerServiceImplBase
{
    private static final Logger logger = LogManager.getLogger(RetinaServerImpl.class);
    private final MetadataService metadataService;

    /**
     * Initialize the visibility management for all the records.
     *
     */
    public RetinaServerImpl()
    {
        this.metadataService = MetadataService.Instance();
        try {
            boolean orderedEnabled = Boolean.parseBoolean(ConfigFactory.Instance().getProperty("executor.ordered.layout.enabled"));
            boolean compactEnabled = Boolean.parseBoolean(ConfigFactory.Instance().getProperty("executor.compact.layout.enabled"));
            List<Schema> schemas = this.metadataService.getSchemas();
            for (Schema schema : schemas) {
                if (schema.getName().equals("tpch_4")) {
                    continue;
                }
                List<Table> tables = this.metadataService.getTables(schema.getName());
                for (Table table : tables) {
                    System.out.println("Schema: " + schema.getName() + " Table: " + table.getName());
                    List<Layout> layouts = this.metadataService.getLayouts(schema.getName(), table.getName());
                    List<String> files = new LinkedList<>();
                    for (Layout layout : layouts) {
                        if (layout.isReadable())
                        {
                            if (orderedEnabled)
                            {
                                String[] orderedPaths = layout.getOrderedPathUris();
                                validateOrderedOrCompactPaths(orderedPaths);
                                Storage storage = StorageFactory.Instance().getStorage(orderedPaths[0]);
                                files.addAll(storage.listPaths(orderedPaths));
                            }
                            if (compactEnabled)
                            {
                                String[] compactPaths = layout.getCompactPathUris();
                                validateOrderedOrCompactPaths(compactPaths);
                                Storage storage = StorageFactory.Instance().getStorage(compactPaths[0]);
                                files.addAll(storage.listPaths(compactPaths));
                            }
                        }
                    }
                    for (String filePath: files)
                    {
                        Storage storage = StorageFactory.Instance().getStorage(filePath);
                        PhysicalReader fsReader = PhysicalReaderUtil.newPhysicalReader(storage, filePath);
                        long fileLen = fsReader.getFileLength();
                        fsReader.seek(fileLen - Long.BYTES);
                        long fileTailOffset = fsReader.readLong(ByteOrder.BIG_ENDIAN);
                        int fileTailLength = (int) (fileLen - fileTailOffset - Long.BYTES);
                        fsReader.seek(fileTailOffset);
                        ByteBuffer fileTailBuffer = fsReader.readFully(fileTailLength);
                        PixelsProto.FileTail fileTail = PixelsProto.FileTail.parseFrom(fileTailBuffer);
                        PixelsProto.PostScript postScript = fileTail.getPostscript();
                        PixelsProto.Footer footer = fileTail.getFooter();
                        System.out.println("File: " + filePath);
                        // row group num
                        int rowGroupNum = footer.getRowGroupInfosCount();
                        System.out.println("Row group num: " + rowGroupNum);
                        // record num
                        int recordNum = postScript.getNumberOfRows();
                        System.out.println("Record num: " + recordNum);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error while initializing RetinaServerImpl", e);
        }
    }

    @Override
    public void updateRecord(RetinaProto.UpdateRecordRequest request,
                             StreamObserver<RetinaProto.UpdateRecordResponse> responseObserver)
    {
        // TODO: Implement updateRecord
    }

    @Override
    public void insertRecord(RetinaProto.InsertRecordRequest request,
                             StreamObserver<RetinaProto.InsertRecordResponse> responseObserver)
    {
        // TODO: Implement insertRecord
    }

    @Override
    public void deleteRecord(RetinaProto.DeleteRecordRequest request,
                             StreamObserver<RetinaProto.DeleteRecordResponse> responseObserver)
    {
        // TODO: Implement DeleteRecord
    }

    @Override
    public void queryRecords(RetinaProto.QueryRecordsRequest request,
                            StreamObserver<RetinaProto.QueryRecordsResponse> responseObserver)
    {
        // TODO: Implement QueryRecords
    }

    @Override
    public void finishRecords(RetinaProto.QueryAck request,
                            StreamObserver<RetinaProto.ResponseHeader> responseObserver)
    {
        // TODO: Implement FinishRecords
    }

    @Override
    public void queryVisibility(RetinaProto.QueryVisibilityRequest request,
                                StreamObserver<RetinaProto.QueryVisibilityResponse> responseObserver)
    {
        // TODO: Implement QueryVisibility
    }

    @Override
    public void finishVisibility(RetinaProto.QueryAck request,
                                StreamObserver<RetinaProto.ResponseHeader> responseObserver)
    {
        // TODO: Implement FinishVisibility
    }

    /**
     * Check if the order or compact paths from pixels metadata is valid.
     * @param paths the order or compact paths from pixels metadata.
     */
    public static void validateOrderedOrCompactPaths(String[] paths)
    {
        requireNonNull(paths, "paths is null");
        checkArgument(paths.length > 0, "paths must contain at least one valid directory");
        try
        {
            Storage.Scheme firstScheme = Storage.Scheme.fromPath(paths[0]);
            for (int i = 1; i < paths.length; ++i)
            {
                Storage.Scheme scheme = Storage.Scheme.fromPath(paths[i]);
                checkArgument(firstScheme.equals(scheme),
                        "all the directories in the paths must have the same storage scheme");
            }
        } catch (Throwable e)
        {
            throw new RuntimeException("failed to parse storage scheme from paths", e);
        }
    }
}
