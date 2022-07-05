/*
 * Copyright 2017 PixelsDB.
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
package io.pixelsdb.pixels.common.utils;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.hotspot.DefaultExports;

/**
 * @author: Joseph
 * @date: Create in 2022-07-05 9:30
 **/

// This client exported data include stats like CPU time spent and memory usage.
public class prometheusClient {
  
    static CollectorRegistry registry;

    public void initialize() throws Exception{
        registry = new CollectorRegistry();
        DefaultExports.initialize();
        DefaultExports.register(registry);
    }

    public Double get_jvm_heap_memory_bytes_used(){
        return registry.getSampleValue("jvm_memory_bytes_used", new String[]{"area"}, new String[]{"heap"});
    }

    public Double get_jvm_nonheap_memory_bytes_used(){
        return registry.getSampleValue("jvm_memory_bytes_used", new String[]{"area"}, new String[]{"nonheap"});
    }

    public Double get_jvm_heap_memory_bytes_max(){
        return registry.getSampleValue("jvm_memory_bytes_max", new String[]{"area"}, new String[]{"heap"});
    }

    public Double get_jvm_nonheap_memory_bytes_max(){
        return registry.getSampleValue("jvm_memory_bytes_max", new String[]{"area"}, new String[]{"nonheap"});
    }

    public Double get_process_cpu_seconds_total(){
        return registry.getSampleValue("process_cpu_seconds_total", new String[]{}, new String[]{});
    }

    public Double Bytes_to_GigaBytes(Double Bytes){
        return Bytes * (1E-9);
    }
}
