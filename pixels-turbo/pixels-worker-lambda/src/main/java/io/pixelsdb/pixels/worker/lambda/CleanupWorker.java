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
package io.pixelsdb.pixels.worker.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import io.pixelsdb.pixels.common.turbo.cleanup.TemporaryResourceManager;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;

/** Separate, periodically invoked management function; never run in a query worker's finally block. */
public final class CleanupWorker implements RequestHandler<Map<String, Object>, String>
{
    @Override
    public String handleRequest(Map<String, Object> event, Context context)
    {
        String journal = ConfigFactory.Instance().getProperty("executor.cleanup.journal");
        if (journal == null || !journal.startsWith("s3://"))
        { throw new IllegalStateException("Lambda cleanup requires a shared executor.cleanup.journal=s3://.../"); }
        try
        {
            if (event != null && event.containsKey("scope"))
            { return TemporaryResourceManager.Instance().snapshot((String) event.get("scope")).toJSONString(); }
            TemporaryResourceManager.Instance().runOnce();
            return "cleanup sweep completed";
        }
        catch (IOException failure) { throw new UncheckedIOException(failure); }
    }
}
