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
package io.pixelsdb.pixels.common.proxy.response;

import io.pixelsdb.pixels.common.metadata.domain.Schema;

import java.util.List;

/**
 * @author hank
 * @create 2023-04-21
 */
public class GetSchemasResponse
{
    private List<Schema> schemas;

    /**
     * Default constructor for Jackson.
     */
    public GetSchemasResponse() { }

    public GetSchemasResponse(List<Schema> schemas)
    {
        this.schemas = schemas;
    }

    public List<Schema> getSchemas()
    {
        return schemas;
    }

    public void setSchemas(List<Schema> schemas)
    {
        this.schemas = schemas;
    }
}
