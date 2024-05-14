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
package io.pixelsdb.pixels.server;

import com.alibaba.fastjson.JSON;
import io.pixelsdb.pixels.common.server.ExecutionHint;
import io.pixelsdb.pixels.common.server.QueryStatus;
import io.pixelsdb.pixels.common.server.rest.request.GetQueryResultRequest;
import io.pixelsdb.pixels.common.server.rest.request.GetQueryStatusRequest;
import io.pixelsdb.pixels.common.server.rest.request.SubmitQueryRequest;
import io.pixelsdb.pixels.common.server.rest.response.GetQueryStatusResponse;
import io.pixelsdb.pixels.common.server.rest.response.SubmitQueryResponse;
import io.pixelsdb.pixels.server.constant.RestUrlPath;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

;

/**
 * @author hank
 * @create 2023-06-04
 */
@SpringBootTest
@AutoConfigureMockMvc
public class TestQueryAPI
{
    @Autowired
    private MockMvc mockMvc;

    @Test
    public void testExecuteQueryBestEffort() throws Exception
    {
        String json1 = this.mockMvc.perform(
                        post(RestUrlPath.SUBMIT_QUERY).contentType(MediaType.APPLICATION_JSON).content(
                                JSON.toJSONString(new SubmitQueryRequest(
                                        "SELECT COUNT(l_orderkey) AS d_l_orderkey FROM tpch.lineitem",
                                        ExecutionHint.RELAXED, 1))))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
        System.out.println(json1);
        SubmitQueryResponse response1 = JSON.parseObject(json1, SubmitQueryResponse.class);

        // wait two seconds to make sure the first query is submitted and running
        TimeUnit.SECONDS.sleep(2);
        String json2 = this.mockMvc.perform(
                        post(RestUrlPath.SUBMIT_QUERY).contentType(MediaType.APPLICATION_JSON).content(
                                JSON.toJSONString(new SubmitQueryRequest(
                                        "SELECT * FROM tpch.nation", ExecutionHint.BEST_OF_EFFORT, 10))))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
        System.out.println(json2);
        SubmitQueryResponse response2 = JSON.parseObject(json2, SubmitQueryResponse.class);

        boolean allFinished;
        do
        {
            allFinished = true;
            String statusJson = this.mockMvc.perform(post(RestUrlPath.GET_QUERY_STATUS)
                            .contentType(MediaType.APPLICATION_JSON).content(
                                    JSON.toJSONString(new GetQueryStatusRequest(Arrays.asList(
                                    response1.getTraceToken(), response2.getTraceToken())))))
                    .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
            GetQueryStatusResponse queryStatus = JSON.parseObject(statusJson, GetQueryStatusResponse.class);
            System.out.println(statusJson);
            for (Map.Entry<String, QueryStatus> entry : queryStatus.getQueryStatuses().entrySet())
            {
                if (entry.getValue() != QueryStatus.FINISHED)
                {
                    allFinished = false;
                    break;
                }
            }
            TimeUnit.SECONDS.sleep(1);
        } while (!allFinished);

        this.mockMvc.perform(post(RestUrlPath.GET_QUERY_RESULT).contentType(MediaType.APPLICATION_JSON).content(
                        JSON.toJSONString(new GetQueryResultRequest(response1.getTraceToken())))).andDo(print())
                .andExpect(status().isOk());

        this.mockMvc.perform(post(RestUrlPath.GET_QUERY_RESULT).contentType(MediaType.APPLICATION_JSON).content(
                        JSON.toJSONString(new GetQueryResultRequest(response2.getTraceToken())))).andDo(print())
                .andExpect(status().isOk());
    }

    @Test
    public void testExecuteQueryRelaxed() throws Exception
    {
        String json = this.mockMvc.perform(
                post(RestUrlPath.SUBMIT_QUERY).contentType(MediaType.APPLICATION_JSON).content(
                        JSON.toJSONString(new SubmitQueryRequest(
                                "SELECT * FROM tpch.nation", ExecutionHint.RELAXED, 10))))
                .andDo(print()).andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
        SubmitQueryResponse response = JSON.parseObject(json, SubmitQueryResponse.class);

        this.mockMvc.perform(post(RestUrlPath.GET_QUERY_STATUS).contentType(MediaType.APPLICATION_JSON).content(
                JSON.toJSONString(new GetQueryStatusRequest(Arrays.asList(response.getTraceToken())))))
                .andDo(print()).andExpect(status().isOk());

        TimeUnit.SECONDS.sleep(5);
        this.mockMvc.perform(post(RestUrlPath.GET_QUERY_STATUS).contentType(MediaType.APPLICATION_JSON).content(
                        JSON.toJSONString(new GetQueryStatusRequest(Arrays.asList(response.getTraceToken())))))
                .andDo(print()).andExpect(status().isOk());

        this.mockMvc.perform(post(RestUrlPath.GET_QUERY_RESULT).contentType(MediaType.APPLICATION_JSON).content(
                JSON.toJSONString(new GetQueryResultRequest(response.getTraceToken())))).andDo(print())
                .andExpect(status().isOk());
    }

    @Test
    public void testExecuteQueryImmediate() throws Exception
    {
        String json = this.mockMvc.perform(
                        post(RestUrlPath.SUBMIT_QUERY).contentType(MediaType.APPLICATION_JSON).content(
                                JSON.toJSONString(new SubmitQueryRequest(
                                        "SELECT * FROM tpch_10g.nation LIMIT 1", ExecutionHint.IMMEDIATE, 10))))
                .andDo(print()).andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
        SubmitQueryResponse response = JSON.parseObject(json, SubmitQueryResponse.class);
        TimeUnit.SECONDS.sleep(5);
        this.mockMvc.perform(post(RestUrlPath.GET_QUERY_RESULT).contentType(MediaType.APPLICATION_JSON).content(
                JSON.toJSONString(new GetQueryResultRequest(response.getTraceToken())))).andDo(print()).andExpect(status().isOk());
    }
}
