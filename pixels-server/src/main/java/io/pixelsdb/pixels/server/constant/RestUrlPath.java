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
package io.pixelsdb.pixels.server.constant;

/**
 * @author hank
 * @create 2023-05-30
 */
public class RestUrlPath
{
    public static final String GET_SCHEMAS = "/api/metadata/get-schemas";
    public static final String GET_TABLES = "/api/metadata/get-tables";
    public static final String GET_COLUMNS = "/api/metadata/get-columns";
    public static final String GET_VIEWS = "/api/metadata/get-views";

    public static final String ESTIMATE_QUERY_COST = "/api/query/estimate-query-cost";
    public static final String SUBMIT_QUERY = "/api/query/submit-query";
    public static final String GET_QUERY_STATUS = "/api/query/get-query-status";
    public static final String GET_QUERY_RESULT = "/api/query/get-query-result";
}
