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
package io.pixelsdb.pixels.amphi;

import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.*;
import io.pixelsdb.pixels.common.physical.Storage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

// Connect to remote metadata service and manipulate on the catalog data
public class TestMetadataService
{
    String hostAddr = "ec2-18-218-128-203.us-east-2.compute.amazonaws.com";

    MetadataService instance = null;

    @Before
    public void init()
    {
        this.instance = MetadataService.CreateInstance(hostAddr, 18888);
    }

    @After
    public void shutdown() throws InterruptedException
    {
        this.instance.shutdown();
    }

    @Test
    public void testPeerApi()
            throws MetadataException, UnknownHostException
    {
        InetAddress inetAddress = InetAddress.getLocalHost();

        // Create peer
        boolean peerCreatedError = instance.createPeer("mac-laptop", "epfl", inetAddress.getHostAddress(), 8889, Storage.Scheme.s3);
        assertFalse(peerCreatedError);
        Peer createdPeer = instance.getPeer("mac-laptop");
        System.out.println("Created a new peer: " + createdPeer.toString());
        assertEquals("mac-laptop", createdPeer.getName());

        // Update peer fields
        createdPeer.setLocation("epfl-dias");
        boolean peerUpdatedError = instance.updatePeer(createdPeer);
        assertFalse(peerUpdatedError);
        Peer updatedPeer = instance.getPeer("mac-laptop");
        System.out.println("Updated the peer: " + updatedPeer);
        assertEquals("epfl-dias", updatedPeer.getLocation());

        System.out.println(updatedPeer.getId());

        // Delete the peer
        assertFalse(instance.deletePeer("mac-laptop"));
    }

    @Test
    public void testGetPaths() throws MetadataException
    {
        // Get path from layout
        Layout lineitemLayout = instance.getLatestLayout("tpch", "lineitem");
        System.out.println(lineitemLayout);
        String [] orderedPaths = lineitemLayout.getOrderedPathUris();
        for (String path : orderedPaths) {
            System.out.println(path);
        }

        // Get compact and order path
        List<Path> lineitemPaths = instance.getPaths(lineitemLayout.getId(), true);
        for (int i = 0; i < lineitemPaths.size(); i++) {
            System.out.println(lineitemPaths.get(i));
            System.out.println(lineitemPaths.get(i).getId());
        }
    }

    @Test
    public void testPeerPathApi()
            throws MetadataException, UnknownHostException
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
        assertEquals(3, partialColumns.size());
        Layout lineitemLayout = instance.getLatestLayout("tpch", "lineitem");
        Path path = lineitemLayout.getOrderedPaths().get(0);
        boolean peerPathCreateError = instance.createPeerPath("/data/lineitem", partialColumns, path, mockPeer);
        assertFalse(peerPathCreateError);

        // Validate catalog
        List<PeerPath> lineitemPeerPaths = instance.getPeerPaths(path.getId(), true);
        assertEquals(1, lineitemPeerPaths.size());
        PeerPath lineitemPeerPath = lineitemPeerPaths.get(0);
        System.out.println("Retrieved lineitem peer path: " + lineitemPeerPath);

        // Clear mock catalog records
        assertFalse(instance.deletePeerPaths(Arrays.asList(lineitemPeerPath.getPathId())));
        assertFalse(instance.deletePeer("mac-laptop"));
    }
}
