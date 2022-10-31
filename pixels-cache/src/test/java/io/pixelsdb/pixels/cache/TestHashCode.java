/*
 * Copyright 2022 PixelsDB.
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
package io.pixelsdb.pixels.cache;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

public class TestHashCode {

    private int hashcode(byte[] bytes) {
        int var1 = 1;
        int var2 = 0;

//        for(int var3 = bytes.length - 1; var3 >= var2; --var3) {
//            var1 = 31 * var1 + bytes[var3];
//        }

        for(int var3 = 0; var3 < bytes.length; ++var3) {
            var1 = 31 * var1 + bytes[var3];
        }

        return var1;
    }
    @Test
    public void testHashPartition() {
        // read the dumpedCache.txt file
        int dataSize = 512000;
        double loadFactor = 0.5f;
        int partitions = 32;
        int[] partitionKeys = new int[partitions];
        long[] partitionSizes = new long[partitions];
        Map<Integer, Set<Short>> partitionColumns = new HashMap<>(partitions);
        Map<Integer, Set<Long>> partitionBlks = new HashMap<>(partitions);
        Map<Integer, Set<Short>> partitionRgs = new HashMap<>(partitions);


        for (int i = 0; i < partitions; ++i) {
            partitionColumns.put(i, new HashSet<>(20));
            partitionBlks.put(i, new HashSet<>(20));
            partitionRgs.put(i, new HashSet<>(20));
        }
        Arrays.fill(partitionKeys, 0);
        Arrays.fill(partitionSizes, 0);
        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(
                    "dumpedCache.txt"));
            String line = reader.readLine();
            while (line != null) {
                String[] keys = line.split(";")[1].split("-");
                long blockId = Long.parseLong(keys[0]);
                short rgId = Short.parseShort(keys[1]);
                short columnId = Short.parseShort(keys[2]);
                int size = Integer.parseInt(line.split(";")[2].split("-")[1]);
                ByteBuffer keyBuf = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN);

                keyBuf.putLong(blockId);
                keyBuf.putShort(rgId);
                keyBuf.putShort(columnId);

                keyBuf.position(0);

//                int hash = keyBuf.hashCode();
                int hash = hashcode(keyBuf.array()) & 0x7fffffff;
//                int hash = Hashing.murmur3_128().hashBytes(keyBuf).asInt() & 0x7fffffff; // better than default hashing
                int partition = hash % partitions;
                partitionKeys[partition] += 1;
                partitionSizes[partition] += size;
                partitionColumns.get(partition).add(columnId);
                partitionBlks.get(partition).add(blockId);
                partitionRgs.get(partition).add(rgId);

                line = reader.readLine();
            }
            System.out.println(Arrays.toString(partitionKeys));
            System.out.println(Arrays.toString(partitionSizes));
            System.out.println("----------------------------------");
            for (int i = 0; i < partitions; ++i) {
                Short[] cols = partitionColumns.get(i).toArray(new Short[0]);
                Arrays.sort(cols);
                System.out.println(Arrays.toString(cols));
            }
            System.out.println("----------------------------------");

            for (int i = 0; i < partitions; ++i) {
                Long[] cols = partitionBlks.get(i).toArray(new Long[0]);
                Arrays.sort(cols);
                System.out.println(Arrays.toString(cols));
            }
            System.out.println("----------------------------------");

            for (int i = 0; i < partitions; ++i) {
                Short[] cols = partitionRgs.get(i).toArray(new Short[0]);
                Arrays.sort(cols);
                System.out.println(Arrays.toString(cols));
            }


            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testHashPartitionOnRgAndCol() {
        // read the dumpedCache.txt file
        int partitions = 16;
        int[] partitionKeys = new int[partitions];
        long[] partitionSizes = new long[partitions];
        Map<Integer, Set<Short>> partitionColumns = new HashMap<>(partitions);
        Map<Integer, Set<Long>> partitionBlks = new HashMap<>(partitions);
        Map<Integer, Set<Short>> partitionRgs = new HashMap<>(partitions);


        for (int i = 0; i < partitions; ++i) {
            partitionColumns.put(i, new HashSet<>(20));
            partitionBlks.put(i, new HashSet<>(20));
            partitionRgs.put(i, new HashSet<>(20));
        }
        Arrays.fill(partitionKeys, 0);
        Arrays.fill(partitionSizes, 0);
        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(
                    "dumpedCache.txt"));
            String line = reader.readLine();
            while (line != null) {
                String[] keys = line.split(";")[1].split("-");
                short rgId = Short.parseShort(keys[1]);
                short columnId = Short.parseShort(keys[2]);
                int size = Integer.parseInt(line.split(";")[2].split("-")[1]);
                ByteBuffer keyBuf = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);

                keyBuf.putShort(rgId);
                keyBuf.putShort(columnId);

                keyBuf.position(0);

//                int hash = keyBuf.hashCode();
                int hash = hashcode(keyBuf.array()) & 0x7fffffff;
//                int hash = Hashing.murmur3_128().hashBytes(keyBuf).asInt() & 0x7fffffff; // better than default hashing
                int partition = hash % partitions;
                partitionKeys[partition] += 1;
                partitionSizes[partition] += size;
                partitionColumns.get(partition).add(columnId);
                partitionRgs.get(partition).add(rgId);

                line = reader.readLine();
            }
            System.out.println(Arrays.toString(partitionKeys));
            System.out.println(Arrays.toString(partitionSizes));
            System.out.println("----------------------------------");
            for (int i = 0; i < partitions; ++i) {
                Short[] cols = partitionColumns.get(i).toArray(new Short[0]);
                Arrays.sort(cols);
                System.out.println(Arrays.toString(cols));
            }
            System.out.println("----------------------------------");

            for (int i = 0; i < partitions; ++i) {
                Long[] cols = partitionBlks.get(i).toArray(new Long[0]);
                Arrays.sort(cols);
                System.out.println(Arrays.toString(cols));
            }
            System.out.println("----------------------------------");

            for (int i = 0; i < partitions; ++i) {
                Short[] cols = partitionRgs.get(i).toArray(new Short[0]);
                Arrays.sort(cols);
                System.out.println(Arrays.toString(cols));
            }


            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void hashOnKeyBuffer() {
        // read the dumpedCache.txt file
        Map<Integer, Integer> hashCodes = new HashMap<>();
        Map<Integer, Integer> hashBuckets = new HashMap<>();
        int dataSize = 512000;
        double loadFactor = 0.5f;
        int tableSize = (int) (((double) dataSize) / loadFactor);
        BufferedReader reader;
        int hashConflictCnt = 0;
        int bucketConflictCnt = 0;
        int total = 0;
        try {
            reader = new BufferedReader(new FileReader(
                    "dumpedCache.txt"));
            String line = reader.readLine();
            while (line != null) {
                String[] keys = line.split(";")[1].split("-");
                long blockId = Long.parseLong(keys[0]);
                short rgId = Short.parseShort(keys[1]);
                short columnId = Short.parseShort(keys[2]);
                ByteBuffer keyBuf = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN);

                keyBuf.putLong(blockId);
                keyBuf.putShort(rgId);
                keyBuf.putShort(columnId);

                keyBuf.position(0);

//                int hash = keyBuf.hashCode();
                int hash = hashcode(keyBuf.array()) & 0x7fffffff;
//                int hash = Hashing.murmur3_128().hashBytes(keyBuf).asInt() & 0x7fffffff; // better than default hashing
                int bucket = hash % tableSize;
                if (hashCodes.containsKey(hash)) {
                    hashConflictCnt += 1;
                    hashCodes.put(hash, hashCodes.get(hash) + 1);
//                    System.out.println("hash code conflict; hash=" + hash + " " + blockId + " " + rgId + " " + columnId);
                } else {
                    hashCodes.put(hash, 1);
                }

                if (hashBuckets.containsKey(bucket)) {
                    bucketConflictCnt += 1;
                    hashBuckets.put(bucket, hashBuckets.get(bucket) + 1);

//                    System.out.println("hash code conflict; bucket=" + bucket + " " + blockId + " " + rgId + " " + columnId);
                } else {
                    hashBuckets.put(bucket, 1);
                }
                line = reader.readLine();
                total += 1;
            }
            System.out.println(hashConflictCnt + " " + bucketConflictCnt + " " + total);

            System.out.println(hashCodes.values().stream().mapToInt(a -> a).max());
            System.out.println(hashBuckets.values().stream().mapToInt(a -> a).max());

            PrintWriter writer = new PrintWriter("hashCollision.txt", "UTF-8");
            for (Map.Entry<Integer, Integer> entry : hashCodes.entrySet()) {
                writer.println(entry.getValue());
            }
            writer.close();

            PrintWriter writer2 = new PrintWriter("bucketCollision.txt", "UTF-8");
            for (Map.Entry<Integer,Integer> entry : hashBuckets.entrySet()) {
                writer2.println(entry.getValue());
            }
            writer2.close();
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
