package io.pixelsdb.pixels.common.physical;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.List;

/**
 * Created at: 20/08/2021
 * Author: hank
 */
public interface Storage
{
    List<Status> listStatus(String path);

    Status getStatus(String path);

    Location getLocation(String path);

    DataInputStream open(String path);

    DataOutputStream create(String path);

    boolean exists(String path);

    boolean isFile(String path);

    boolean isDirectory(String path);
}
