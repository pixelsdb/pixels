/*
 * Copyright 2026 PixelsDB.
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
package io.pixelsdb.pixels.planner;

import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.File;
import io.pixelsdb.pixels.common.metadata.domain.Path;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestTableFileSnapshot
{
    @Test
    public void testLoadsRegularAndVirtualFilesOncePerPath() throws Exception
    {
        MetadataService metadataService = mock(MetadataService.class);
        Path path = new Path();
        path.setId(7);
        path.setUri("s3://bucket/table/ordered");

        File regular = new File();
        regular.setId(11);
        regular.setName("regular.pxl");
        regular.setType(File.Type.REGULAR);
        File virtual = new File();
        virtual.setId(12);
        virtual.setName("virtual.pxl");
        virtual.setType(File.Type.VIRTUAL);

        when(metadataService.getFilesByType(
                7, EnumSet.of(File.Type.REGULAR, File.Type.VIRTUAL)))
                .thenReturn(Arrays.asList(regular, virtual));

        TableFileSnapshot snapshot = new TableFileSnapshot(metadataService);
        assertEquals(
                Collections.singletonList("s3://bucket/table/ordered/regular.pxl"),
                snapshot.regularFilePaths(Collections.singletonList(path)));
        assertEquals(2, snapshot.files(Collections.singletonList(path)).size());

        verify(metadataService, times(1)).getFilesByType(
                7, EnumSet.of(File.Type.REGULAR, File.Type.VIRTUAL));
    }
}
