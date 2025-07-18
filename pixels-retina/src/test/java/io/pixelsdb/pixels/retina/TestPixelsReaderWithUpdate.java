/*
 * Copyright 2025 PixelsDB.
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
package io.pixelsdb.pixels.retina;

import com.alibaba.fastjson.JSON;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.File;
import io.pixelsdb.pixels.common.metadata.domain.Layout;
import io.pixelsdb.pixels.common.metadata.domain.Path;
import io.pixelsdb.pixels.common.retina.RetinaService;
import io.pixelsdb.pixels.common.server.ExecutionHint;
import io.pixelsdb.pixels.common.server.rest.request.GetQueryResultRequest;
import io.pixelsdb.pixels.common.server.rest.request.SubmitQueryRequest;
import io.pixelsdb.pixels.common.server.rest.response.GetQueryResultResponse;
import io.pixelsdb.pixels.common.server.rest.response.SubmitQueryResponse;
import io.pixelsdb.pixels.server.controller.QueryController;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

public class TestPixelsReaderWithUpdate
{
    private QueryController queryController = new QueryController();

    private final RetinaService retinaService = RetinaService.Instance();
    private final MetadataService metadataService = MetadataService.Instance();
    private final String schemaName = "tpch";
    private final String tableName = "nation";
    private File file;

    {
        try
        {
            Layout layout = this.metadataService.getLatestLayout(schemaName, tableName);
            Path path = layout.getOrderedPaths().get(0);
            this.file = this.metadataService.getFiles(path.getId()).get(0);
        } catch (MetadataException e)
        {
            System.out.println("failed to get fileId.");
        }
    }

    public void query()
    {
        try
        {
            String queryStr = String.format("select * from %s.%s", schemaName, tableName);
            SubmitQueryRequest submitQueryRequest = new SubmitQueryRequest(queryStr,
                    ExecutionHint.IMMEDIATE, 25);

            SubmitQueryResponse submitQueryResponse = queryController.submitQuery(submitQueryRequest);
            TimeUnit.SECONDS.sleep(5);

            GetQueryResultRequest resultRequest = new GetQueryResultRequest(submitQueryResponse.getTraceToken());
            GetQueryResultResponse resultResponse = queryController.getQueryResult(resultRequest);
            System.out.println("Query Result:");
            for (String[] row : resultResponse.getRows())
            {
                System.out.println(JSON.toJSONString(row));
            }
        } catch (Exception e)
        {
            System.out.println("failed to query.");
        }
    }

    @Test
    public void testDeletion()
    {
        System.out.println(this.file.getId());
        query();
    }
}
