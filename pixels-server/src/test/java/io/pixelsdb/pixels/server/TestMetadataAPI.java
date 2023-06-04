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
import io.pixelsdb.pixels.common.server.rest.request.GetSchemasRequest;
import io.pixelsdb.pixels.server.constant.RestUrlPath;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * @author hank
 * @create 2023-04-21
 */
@SpringBootTest
@AutoConfigureMockMvc
public class TestMetadataAPI
{
    @Autowired
    private MockMvc mockMvc;

    @Test
    public void testGetSchemas() throws Exception
    {
        this.mockMvc.perform(
                post(RestUrlPath.GET_SCHEMAS).contentType(MediaType.APPLICATION_JSON).content(
                        JSON.toJSONString(new GetSchemasRequest("hank"))))
                .andDo(print()).andExpect(status().isOk());
    }
}
