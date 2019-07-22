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