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
import io.pixelsdb.pixels.common.server.rest.request.EstimateCostRequest;
import io.pixelsdb.pixels.common.server.rest.request.GetResultRequest;
import io.pixelsdb.pixels.common.server.rest.request.SubmitQueryRequest;
import io.pixelsdb.pixels.common.server.rest.response.EstimateCostResponse;
import io.pixelsdb.pixels.common.server.rest.response.GetResultResponse;
import io.pixelsdb.pixels.common.server.rest.response.SubmitQueryResponse;
import io.pixelsdb.pixels.server.constant.RestUrlPath;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * The REST methods for query execution, scheduling, and cost estimation.
 * @author hank
 * @create 2023-05-30
 */
@RestController
public class QueryController
{
    @PostMapping(value = RestUrlPath.ESTIMATE_COST,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public EstimateCostResponse estimateCost(@RequestBody EstimateCostRequest request)
    {
        // TODO: implement estimate cost.
        return new EstimateCostResponse(ErrorCode.QUERY_SERVER_NOT_SUPPORTED,
                "estimate cost is not supported", 0, 0);
    }

    @PostMapping(value = RestUrlPath.SUBMIT_QUERY,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public SubmitQueryResponse submitQuery(@RequestBody SubmitQueryRequest request)
    {
        return QueryManager.Instance().submitQuery(request);
    }

    @PostMapping(value = RestUrlPath.GET_RESULT,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public GetResultResponse getResult(@RequestBody GetResultRequest request)
    {
        return QueryManager.Instance().popQueryResult(request.getCallbackToken());
    }
}
