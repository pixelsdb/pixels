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

import io.pixelsdb.pixels.common.error.ErrorCode;
import io.pixelsdb.pixels.common.server.QueryStatus;
import io.pixelsdb.pixels.common.server.rest.request.EstimateQueryCostRequest;
import io.pixelsdb.pixels.common.server.rest.request.GetQueryResultRequest;
import io.pixelsdb.pixels.common.server.rest.request.GetQueryStatusRequest;
import io.pixelsdb.pixels.common.server.rest.request.SubmitQueryRequest;
import io.pixelsdb.pixels.common.server.rest.response.EstimateQueryCostResponse;
import io.pixelsdb.pixels.common.server.rest.response.GetQueryResultResponse;
import io.pixelsdb.pixels.common.server.rest.response.GetQueryStatusResponse;
import io.pixelsdb.pixels.common.server.rest.response.SubmitQueryResponse;
import io.pixelsdb.pixels.server.constant.RestUrlPath;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * The REST methods for query execution, scheduling, and cost estimation.
 * @author hank
 * @create 2023-05-30
 * @update 2023-07-23 add {@link #getQueryStatus(GetQueryStatusRequest)}.
 */
@RestController
public class QueryController
{
    @PostMapping(value = RestUrlPath.ESTIMATE_QUERY_COST,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public EstimateQueryCostResponse estimateQueryCost(@RequestBody EstimateQueryCostRequest request)
    {
        // TODO: implement estimate cost.
        return new EstimateQueryCostResponse(ErrorCode.QUERY_SERVER_NOT_SUPPORTED,
                "estimate cost is not supported", 0, 0);
    }

    @PostMapping(value = RestUrlPath.SUBMIT_QUERY,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public SubmitQueryResponse submitQuery(@RequestBody SubmitQueryRequest request)
    {
        return QueryManager.Instance().submitQuery(request);
    }

    @PostMapping(value = RestUrlPath.GET_QUERY_STATUS,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public GetQueryStatusResponse getQueryStatus(@RequestBody GetQueryStatusRequest request)
    {
        if (request.getTraceTokens() == null || request.getTraceTokens().isEmpty())
        {
            return new GetQueryStatusResponse(ErrorCode.QUERY_SERVER_BAD_REQUEST,
                    "traceTokens is null or empty", Collections.emptyMap());
        }
        Map<String, QueryStatus> queryStatuses = new HashMap<>(request.getTraceTokens().size());
        for (String traceToken : request.getTraceTokens())
        {
            queryStatuses.put(traceToken, QueryManager.Instance().getQueryStatus(traceToken));
        }
        return new GetQueryStatusResponse(ErrorCode.SUCCESS, "", queryStatuses);
    }

    @PostMapping(value = RestUrlPath.GET_QUERY_RESULT,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public GetQueryResultResponse getQueryResult(@RequestBody GetQueryResultRequest request)
    {
        GetQueryResultResponse response = QueryManager.Instance().popQueryResult(request.getTraceToken());
        if (response != null)
        {
            return response;
        }
        return new GetQueryResultResponse(ErrorCode.QUERY_SERVER_QUERY_RESULT_NOT_FOUND,
                "the query is not finished yet or its result has been cleared");
    }
}
