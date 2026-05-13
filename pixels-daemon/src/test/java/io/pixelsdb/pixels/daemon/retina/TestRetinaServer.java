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

import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.exception.RetinaException;
import io.pixelsdb.pixels.common.index.service.IndexService;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.File;
import io.pixelsdb.pixels.common.metadata.domain.Layout;
import io.pixelsdb.pixels.common.metadata.domain.Path;
import io.pixelsdb.pixels.common.metadata.domain.Permission;
import io.pixelsdb.pixels.common.metadata.domain.Schema;
import io.pixelsdb.pixels.common.metadata.domain.Table;
import io.pixelsdb.pixels.daemon.ServerContainer;
import io.pixelsdb.pixels.daemon.metadata.MetadataServer;
import io.pixelsdb.pixels.retina.RetinaResourceManager;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.InOrder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestRetinaServer
{
    @Ignore("Integration test requires real metadata server, metadata DB, and fixed local ports.")
    @Test
    public void testRetinaServer()
    {
        ServerContainer container = new ServerContainer();
        MetadataServer metadataServer = new MetadataServer(18888);
        container.addServer("metadata server", metadataServer);
        RetinaServer retinaServer = new RetinaServer(18890);
        container.addServer("retina server", retinaServer);
    }

    @Test
    public void testRetinaServerImplInitializationFailureIsFailClosed() throws Exception
    {
        MetadataService metadataService = mock(MetadataService.class);
        IndexService indexService = mock(IndexService.class);
        RetinaResourceManager resourceManager = mock(RetinaResourceManager.class);

        when(metadataService.getSchemas()).thenThrow(new MetadataException("metadata unavailable"));

        try
        {
            RetinaServerImpl server = new RetinaServerImpl(metadataService, indexService, resourceManager);
            fail("RetinaServerImpl must fail closed when initialization fails: " + server);
        }
        catch (IllegalStateException e)
        {
            assertTrue(e.getMessage().contains("Failed to initialize RetinaServerImpl"));
        }

        verify(resourceManager).recoverCheckpoints();
        verify(resourceManager, never()).startBackgroundGc();
    }

    @Test
    public void testRetinaServerImplStartsBackgroundGcAfterSuccessfulInitialization() throws Exception
    {
        MetadataService metadataService = mock(MetadataService.class);
        IndexService indexService = mock(IndexService.class);
        RetinaResourceManager resourceManager = mock(RetinaResourceManager.class);

        Schema schema = new Schema();
        schema.setName("gc_schema");
        Table table = new Table();
        table.setName("gc_table");
        Path orderedPath = new Path();
        orderedPath.setId(11L);
        orderedPath.setUri("file:///tmp/pixels/ordered");
        Path compactPath = new Path();
        compactPath.setId(12L);
        compactPath.setUri("file:///tmp/pixels/compact");
        Layout layout = new Layout();
        layout.setPermission(Permission.READ_WRITE);
        layout.setOrderedPaths(Collections.singletonList(orderedPath));
        layout.setCompactPaths(Collections.singletonList(compactPath));
        File orderedFile = new File();
        orderedFile.setName("ordered.pxl");
        File compactFile = new File();
        compactFile.setName("compact.pxl");
        List<String> lifecycleEvents = Collections.synchronizedList(new ArrayList<>());

        when(metadataService.getSchemas()).thenReturn(Collections.singletonList(schema));
        when(metadataService.getTables(schema.getName())).thenReturn(Collections.singletonList(table));
        when(metadataService.getLayouts(schema.getName(), table.getName())).thenReturn(Collections.singletonList(layout));
        when(metadataService.getFiles(orderedPath.getId())).thenReturn(Collections.singletonList(orderedFile));
        when(metadataService.getFiles(compactPath.getId())).thenReturn(Collections.singletonList(compactFile));
        doAnswer(invocation -> {
            lifecycleEvents.add("recover");
            return null;
        }).when(resourceManager).recoverCheckpoints();
        doAnswer(invocation -> {
            lifecycleEvents.add("visibility:" + invocation.getArgument(0));
            return null;
        }).when(resourceManager).addVisibility(org.mockito.ArgumentMatchers.anyString());
        doAnswer(invocation -> {
            lifecycleEvents.add("writeBuffer");
            return null;
        }).when(resourceManager).addWriteBuffer(schema.getName(), table.getName());
        doAnswer(invocation -> {
            lifecycleEvents.add("startGc");
            return null;
        }).when(resourceManager).startBackgroundGc();

        new RetinaServerImpl(metadataService, indexService, resourceManager);

        assertTrue(lifecycleEvents.indexOf("recover") >= 0);
        assertTrue(lifecycleEvents.contains("visibility:file:///tmp/pixels/ordered/ordered.pxl"));
        assertTrue(lifecycleEvents.contains("visibility:file:///tmp/pixels/compact/compact.pxl"));
        int writeBufferIndex = lifecycleEvents.indexOf("writeBuffer");
        assertTrue(writeBufferIndex > lifecycleEvents.indexOf("recover"));
        assertTrue(writeBufferIndex > lifecycleEvents.indexOf("visibility:file:///tmp/pixels/ordered/ordered.pxl"));
        assertTrue(writeBufferIndex > lifecycleEvents.indexOf("visibility:file:///tmp/pixels/compact/compact.pxl"));
        assertTrue(lifecycleEvents.indexOf("startGc") > writeBufferIndex);
        verify(resourceManager).addVisibility("file:///tmp/pixels/ordered/ordered.pxl");
        verify(resourceManager).addVisibility("file:///tmp/pixels/compact/compact.pxl");
        verify(resourceManager).startBackgroundGc();
    }

    @Test
    public void testRetinaServerImplBackgroundGcStartFailureIsFailClosed() throws Exception
    {
        MetadataService metadataService = mock(MetadataService.class);
        IndexService indexService = mock(IndexService.class);
        RetinaResourceManager resourceManager = mock(RetinaResourceManager.class);

        when(metadataService.getSchemas()).thenReturn(Collections.emptyList());
        doThrow(new RetinaException("gc disabled by invalid lifecycle"))
                .when(resourceManager).startBackgroundGc();

        try
        {
            RetinaServerImpl server = new RetinaServerImpl(metadataService, indexService, resourceManager);
            fail("RetinaServerImpl must fail closed when background GC cannot start: " + server);
        }
        catch (IllegalStateException e)
        {
            assertTrue(e.getMessage().contains("Failed to initialize RetinaServerImpl"));
        }

        InOrder inOrder = inOrder(resourceManager);
        inOrder.verify(resourceManager).recoverCheckpoints();
        inOrder.verify(resourceManager).startBackgroundGc();
    }
}
