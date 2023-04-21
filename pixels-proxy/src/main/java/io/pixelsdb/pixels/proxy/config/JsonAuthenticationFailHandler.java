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
package io.pixelsdb.pixels.proxy.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.pixelsdb.pixels.proxy.constant.HttpStatus;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.authentication.AuthenticationFailureHandler;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class JsonAuthenticationFailHandler implements AuthenticationFailureHandler
{
    @Override
    public void onAuthenticationFailure(
            HttpServletRequest request, HttpServletResponse response,
            AuthenticationException exception) throws IOException
    {
        Map<String, Object> result = new HashMap<>();
        result.put("msg", "invalid username or password！");
        result.put("code", HttpStatus.ERROR);
        result.put("exception", exception.getMessage());
        response.setContentType("application/json;charset=UTF-8");
        String jsonData = new ObjectMapper().writeValueAsString(result);
        response.getWriter().write(jsonData);
    }
}
