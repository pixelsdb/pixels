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
import io.pixelsdb.pixels.common.server.rest.request.GetResultRequest;
import io.pixelsdb.pixels.common.server.rest.request.SubmitQueryRequest;
import io.pixelsdb.pixels.common.server.rest.response.SubmitQueryResponse;
import io.pixelsdb.pixels.server.constant.RestUrlPath;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

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
    public void testExecuteQueryCostEffective() throws Exception
    {
        String json = this.mockMvc.perform(
                post(RestUrlPath.SUBMIT_QUERY).contentType(MediaType.APPLICATION_JSON).content(
                        JSON.toJSONString(new SubmitQueryRequest(
                                "select * from nation", ExecutionHint.COST_EFFECTIVE, 10))))
                .andDo(print()).andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
        SubmitQueryResponse response = JSON.parseObject(json, SubmitQueryResponse.class);
        TimeUnit.SECONDS.sleep(5);
        this.mockMvc.perform(post(RestUrlPath.GET_RESULT).contentType(MediaType.APPLICATION_JSON).content(
                JSON.toJSONString(new GetResultRequest(response.getCallbackToken())))).andDo(print()).andExpect(status().isOk());
    }

    @Test
    public void testExecuteQueryImmediate() throws Exception
    {
        String json = this.mockMvc.perform(
                        post(RestUrlPath.SUBMIT_QUERY).contentType(MediaType.APPLICATION_JSON).content(
                                JSON.toJSONString(new SubmitQueryRequest(
                                        "select * from nation", ExecutionHint.IMMEDIATE, 10))))
                .andDo(print()).andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
        SubmitQueryResponse response = JSON.parseObject(json, SubmitQueryResponse.class);
        TimeUnit.SECONDS.sleep(5);
        this.mockMvc.perform(post(RestUrlPath.GET_RESULT).contentType(MediaType.APPLICATION_JSON).content(
                JSON.toJSONString(new GetResultRequest(response.getCallbackToken())))).andDo(print()).andExpect(status().isOk());
    }
}
