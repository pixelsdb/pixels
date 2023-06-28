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
package io.pixelsdb.pixels.amphi.coordinator;

import io.pixelsdb.pixels.amphi.TpchQuery;
import io.pixelsdb.pixels.common.exception.AmphiException;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.*;
import io.pixelsdb.pixels.common.physical.Storage;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

// Mock peer and workload to test the decision of coordinator
public class TestCoordinator
{
    String hostAddr = "ec2-18-218-128-203.us-east-2.compute.amazonaws.com";

    MetadataService instance = null;

    @Before
    public void init()
    {
        this.instance = new MetadataService(hostAddr, 18888);
    }

    @After
    public void shutdown() throws InterruptedException
    {
        this.instance.shutdown();
    }

    @Test
    public void testCoordinatorDecision()
            throws IOException, MetadataException, NoSuchFieldException, IllegalAccessException, SqlParseException, AmphiException, InterruptedException
    {
        InetAddress inetAddress = InetAddress.getLocalHost();

        boolean peerCreatedError = instance.createPeer("mac-laptop", "epfl", inetAddress.getHostAddress(), 8889, Storage.Scheme.s3);
        assertFalse(peerCreatedError);
        Peer mockPeer = instance.getPeer("mac-laptop");
        System.out.println("Created a new peer: " + mockPeer.toString());
        assertEquals("mac-laptop", mockPeer.getName());

        List<PeerPath> peerPaths = instance.getPeerPaths(mockPeer.getId(), false);
        assertEquals(0, peerPaths.size());

        // Mock peer path of storing a subset of all columns
        List<Column> allColumns = instance.getColumns("tpch", "lineitem", false);
        assertEquals(16, allColumns.size());
        List<Column> partialColumns = allColumns.stream()
                .filter(column -> (column.getName().equals("l_shipdate") || column.getName().equals("l_quantity")
                        || column.getName().equals("l_discount") || column.getName().equals("l_extendedprice")))
                .collect(Collectors.toList());
        assertEquals(4, partialColumns.size());
        Layout lineitemLayout = instance.getLatestLayout("tpch", "lineitem");
        Path path = lineitemLayout.getOrderedPaths().get(0);
        boolean peerPathCreateError = instance.createPeerPath("/data/lineitem", partialColumns, path, mockPeer);
        assertFalse(peerPathCreateError);

        // Validate catalog
        List<PeerPath> lineitemPeerPaths = instance.getPeerPaths(path.getId(), true);
        assertEquals(1, lineitemPeerPaths.size());
        PeerPath lineitemPeerPath = lineitemPeerPaths.get(0);
        System.out.println("Retrieved lineitem peer path: " + lineitemPeerPath);

        // Test coordinator decision on Q6 (peer able to execute)
        Coordinator coordinator = new Coordinator(this.instance);
        assertFalse(coordinator.decideInCloud(TpchQuery.getQuery(6), "tpch", "mac-laptop"));
        assertTrue(coordinator.decideInCloud(TpchQuery.getQuery(1), "tpch", "mac-laptop"));

        // Clear mock catalog records
        assertFalse(instance.deletePeerPaths(Arrays.asList(lineitemPeerPath.getPathId())));
        assertFalse(instance.deletePeer("mac-laptop"));
    }

}
