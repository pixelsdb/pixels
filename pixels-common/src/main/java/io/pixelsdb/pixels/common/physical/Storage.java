/*
 * Copyright 2021 PixelsDB.
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
package io.pixelsdb.pixels.common.physical;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

/**
 * Created at: 20/08/2021
 * Author: hank
 */
public interface Storage
{
    String getScheme();

    List<Status> listStatus(String path) throws IOException;

    List<String> listPaths(String path) throws IOException;

    Status getStatus(String path) throws IOException;

    long getId(String path) throws IOException;

    List<Location> getLocations(String path) throws IOException;

    String[] getHosts(String path) throws IOException;

    DataInputStream open(String path) throws IOException;

    DataOutputStream create(String path, boolean overwrite,
                            int bufferSize, short replication) throws IOException;

    boolean delete(String path, boolean recursive) throws IOException;

    boolean exists(String path) throws IOException;

    boolean isFile(String path) throws IOException;

    boolean isDirectory(String path) throws IOException;
}
