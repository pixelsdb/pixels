/*
 * This file is copied from MappedBus:
 *
 *   Copyright 2015 Caplogic AB.
 *   Licensed under the Apache License, Version 2.0.
 */
package io.pixelsdb.pixels.cache.mq;

/**
 * Class with constants.
 */
class MappedBusConstants
{

    static class Structure
    {

        public static final int Limit = 0;

        public static final int Data = Length.Limit;

    }

    static class Length
    {

        public static final int Limit = 8;

        public static final int Commit = 1;

        public static final int Rollback = 1;

        public static final int Metadata = 4;

        public static final int StatusFlags = Commit + Rollback;

        public static final int RecordHeader = Commit + Rollback + Metadata;

    }

    static class Commit
    {

        public static final byte NotSet = 0;

        public static final byte Set = 1;

    }

    static class Rollback
    {

        public static final byte NotSet = 0;

        public static final byte Set = 1;

    }
}