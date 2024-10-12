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
package io.pixelsdb.pixels.server.controller;

import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.server.rest.request.GetColumnsRequest;
import io.pixelsdb.pixels.common.server.rest.request.GetSchemasRequest;
import io.pixelsdb.pixels.common.server.rest.request.GetTablesRequest;
import io.pixelsdb.pixels.common.server.rest.request.GetViewsRequest;
import io.pixelsdb.pixels.common.server.rest.response.GetColumnsResponse;
import io.pixelsdb.pixels.common.server.rest.response.GetSchemasResponse;
import io.pixelsdb.pixels.common.server.rest.response.GetTablesResponse;
import io.pixelsdb.pixels.common.server.rest.response.GetViewsResponse;
import io.pixelsdb.pixels.server.constant.RestUrlPath;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/**
 * @author hank
 * @create 2023-04-21
 */
@RestController
public class MetadataController
{
    private static final MetadataService metadataService;

    static
    {
        metadataService = MetadataService.Instance();
    }

    @PostMapping(value = RestUrlPath.GET_SCHEMAS,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public GetSchemasResponse getSchemas(@RequestBody GetSchemasRequest request)
    {
        // TODO: check username.
        try
        {
            return new GetSchemasResponse(metadataService.getSchemas());
        } catch (MetadataException e)
        {
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(), e);
        }
    }

    @PostMapping(value = RestUrlPath.GET_TABLES,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public GetTablesResponse getTables(@RequestBody GetTablesRequest request)
    {
        String schemaName = request.getSchemaName();
        try
        {
            return new GetTablesResponse(schemaName, metadataService.getTables(schemaName));
        } catch (MetadataException e)
        {
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(), e);
        }
    }

    @PostMapping(value = RestUrlPath.GET_COLUMNS,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public GetColumnsResponse getColumns(@RequestBody GetColumnsRequest request)
    {
        String schemaName = request.getSchemaName();
        String tableName = request.getTableName();
        try
        {
            return new GetColumnsResponse(schemaName, tableName,
                    metadataService.getColumns(schemaName, tableName, true));
        } catch (MetadataException e)
        {
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(), e);
        }
    }

    @PostMapping(value = RestUrlPath.GET_VIEWS,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public GetViewsResponse getViews(@RequestBody GetViewsRequest request)
    {
        String schemaName = request.getSchemaName();
        try
        {
            return new GetViewsResponse(schemaName, metadataService.getViews(schemaName));
        } catch (MetadataException e)
        {
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(), e);
        }
    }
}
