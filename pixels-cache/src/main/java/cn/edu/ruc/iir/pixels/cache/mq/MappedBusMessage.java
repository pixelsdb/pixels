/* 
* Copyright 2015 Caplogic AB. 
* 
* Licensed under the Apache License, Version 2.0 (the "License"); 
* you may not use this file except in compliance with the License. 
* You may obtain a copy of the License at 
* 
* http://www.apache.org/licenses/LICENSE-2.0 
* 
* Unless required by applicable law or agreed to in writing, software 
* distributed under the License is distributed on an "AS IS" BASIS, 
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
* See the License for the specific language governing permissions and 
* limitations under the License. 
*/
package cn.edu.ruc.iir.pixels.cache.mq;

import cn.edu.ruc.iir.pixels.cache.MemoryMappedFile;

/**
 * Interface for messages that can be serialized to the bus.
 *
 */
public interface MappedBusMessage {

    /**
     * Writes a message to the bus.
     *
     * @param mem an instance of the memory mapped file
     * @param pos the start of the current record
     */
    public void write(MemoryMappedFile mem, long pos);
    
    /**
     * Reads a message from the bus.
     * 
     * @param mem an instance of the memory mapped file
     * @param pos the start of the current record
     */
    public void read(MemoryMappedFile mem, long pos);
    
    /**
     * Returns the message type.
     *
     * @return the message type
     */
    public int type();
}