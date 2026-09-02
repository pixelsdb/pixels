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
package io.pixelsdb.pixels.cli.executor;

import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.daemon.metadata.MetadataDbType;
import io.pixelsdb.pixels.daemon.metadata.MetadataSchemaInitializer;
import net.sourceforge.argparse4j.inf.Namespace;

/**
 * Create metadata tables in the configured metadata database.
 *
 * @author hank
 * @create 2026-09-02
 */
public class InitMetaExecutor implements CommandExecutor
{
    @Override
    public void execute(Namespace ns, String command) throws Exception
    {
        ConfigFactory config = ConfigFactory.Instance();
        String driver = config.getProperty("metadata.db.driver");
        String url = config.getProperty("metadata.db.url");
        MetadataDbType dbType = MetadataDbType.from(driver, url);
        System.out.println("Initializing metadata tables in " + dbType + " database...");
        System.out.println("JDBC URL: " + url);
        int executed = MetadataSchemaInitializer.initialize();
        System.out.println("INIT-META finished, executed " + executed +
                " statement(s) from " + dbType.getSchemaResource());
    }
}
