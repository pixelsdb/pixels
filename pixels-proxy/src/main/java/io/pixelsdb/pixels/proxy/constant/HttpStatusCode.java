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
package io.pixelsdb.pixels.proxy.constant;

/**
 * Http status code
 */
public class HttpStatusCode
{
    /**
     * operation is successful
     */
    public static final int SUCCESS = 200;

    /**
     * create object successfully
     */
    public static final int CREATED = 201;

    /**
     * request has been accepted
     */
    public static final int ACCEPTED = 202;

    /**
     * operation is successful, but not returned data
     */
    public static final int NO_CONTENT = 204;

    /**
     * resource has been removed
     */
    public static final int MOVED_PERM = 301;

    /**
     * redirect
     */
    public static final int SEE_OTHER = 303;

    /**
     * resource is not modified
     */
    public static final int NOT_MODIFIED = 304;

    /**
     * invalid parameters (missing parameters or wrong format)
     */
    public static final int BAD_REQUEST = 400;

    /**
     * not authorized
     */
    public static final int UNAUTHORIZED = 401;

    /**
     * access is forbidden or authorization has expired
     */
    public static final int FORBIDDEN = 403;

    /**
     * resource or service not found
     */
    public static final int NOT_FOUND = 404;

    /**
     * invalid http method
     */
    public static final int BAD_METHOD = 405;

    /**
     * resource conflict or resource been locked
     */
    public static final int CONFLICT = 409;

    /**
     * unsupported data or media type
     */
    public static final int UNSUPPORTED_TYPE = 415;

    /**
     * system internal error
     */
    public static final int ERROR = 500;

    /**
     * interface not implemented
     */
    public static final int NOT_IMPLEMENTED = 501;

    /**
     * system warning
     */
    public static final int WARN = 601;
}
