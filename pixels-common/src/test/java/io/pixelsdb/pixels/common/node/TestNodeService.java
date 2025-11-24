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

package io.pixelsdb.pixels.common.node;

import io.pixelsdb.pixels.daemon.NodeProto;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class TestNodeService
{
    private static final Logger logger = LoggerFactory.getLogger(TestNodeService.class);
    @Test
    public void testGetRetinaList()
    {
        NodeService nodeService = NodeService.Instance();
        List<NodeProto.NodeInfo> retinaList = nodeService.getRetinaList();
        logger.debug("Retina List Size: {}", retinaList.size());
        for(NodeProto.NodeInfo nodeInfo : retinaList)
        {
            logger.debug(nodeInfo.toString());
        }
    }

    @Test
    public void testGetRetinaByBucketId()
    {
        NodeService nodeService = NodeService.Instance();
        NodeProto.NodeInfo retinaByBucket = nodeService.getRetinaByBucket(1);
        logger.info("Retina By Bucket: {}", retinaByBucket);
    }
}
