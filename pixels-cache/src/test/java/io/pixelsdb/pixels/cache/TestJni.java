/*
 * Copyright 2019 PixelsDB.
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

/**
 * Created at: 19-5-11
 * Author: hank
 */
public class TestJni
{
    static
    {
        System.load("/home/hank/dev/idea-projects/pixels/pixels-cache/src/test/c++/test_jni.so");

    }

    public static native void sayHello();

    public static native int add(int a);

    public static native byte[] echo(byte[] bytes, int length);

    public static void main(String[] args)
    {
        byte[] bytes = (
                "hello").getBytes();
        System.out.println(System.getProperty("java.library.path"));
        long start = System.nanoTime();
        //for (int i = 0; i < 1000_000_00; ++i)
        {
            //TestJni.sayHello();
            //System.out.println(TestJni.add(8));
            //System.out.println(new String(TestJni.echo(bytes, bytes.length)));
            //System.out.println(TestJni.add(8));
            //System.out.println(new String(TestJni.echo(bytes, bytes.length)));
            //System.out.println(TestJni.add(8));
            System.out.println(TestJni.echo(bytes, bytes.length).length);
        }
        long end = System.nanoTime();
        System.out.println((end - start) / 1000);
    }
}
