/*
 * Copyright 2025 PixelsDB.
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
package io.pixelsdb.pixels.daemon.retina;

import io.pixelsdb.pixels.daemon.ServerContainer;
import io.pixelsdb.pixels.daemon.metadata.MetadataServer;
import org.junit.Test;

public class TestRetinaServer
{
    @Test
    public void testRetinaServer()
    {
        ServerContainer container = new ServerContainer();
        MetadataServer metadataServer = new MetadataServer(18888);
        container.addServer("metadata server", metadataServer);
        RetinaServer retinaServer = new RetinaServer(18890);
        container.addServer("retina server", retinaServer);
    }
}
