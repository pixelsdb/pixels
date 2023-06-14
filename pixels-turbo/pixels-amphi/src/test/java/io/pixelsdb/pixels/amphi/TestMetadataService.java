package io.pixelsdb.pixels.amphi;

import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.Peer;
import io.pixelsdb.pixels.common.metadata.domain.Schema;

import io.pixelsdb.pixels.common.physical.Storage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.net.InetAddress;
import java.net.UnknownHostException;

// Connect to remote metadata service and manipulate on the catalog data
public class TestMetadataService
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
    public void testPeerApi() throws MetadataException, UnknownHostException
    {
        List<Schema> schemas = instance.getSchemas();
        for(Schema s : schemas)
        {
            System.out.println(s.toString());
        }

        InetAddress inetAddress = InetAddress.getLocalHost();
        System.out.println(inetAddress.getHostName());
        System.out.println(inetAddress.getHostAddress());

//        // Create peer
//        boolean peerCreated = instance.createPeer("mac-laptop", "epfl", inetAddress.getHostAddress(), 8889, Storage.Scheme.s3);
//        if (peerCreated) {
//            System.out.println("Created peer success");
//        }

        // Delete peer


        // Get peer
        Peer createdPeer = instance.getPeer("mac-laptop");
        System.out.println(createdPeer.toString());


    }



}
