package io.pixelsdb.pixels.common.physical.storage;

import io.pixelsdb.pixels.common.physical.Status;
import io.pixelsdb.pixels.common.physical.Storage;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

public class Mock implements Storage {

    public Mock() {
        // read the tmp.txt
    }

    @Override
    public Scheme getScheme() {
        return Storage.Scheme.mock;
    }

    @Override
    public String ensureSchemePrefix(String path) throws IOException {
        return path;
    }

    @Override
    public List<Status> listStatus(String path) throws IOException {
        return null;
    }

    @Override
    public List<String> listPaths(String path) throws IOException {
        return null;
    }

    @Override
    public Status getStatus(String path) throws IOException {
        return null;
    }

    @Override
    public long getFileId(String path) throws IOException {
        return 0;
    }

    @Override
    public boolean mkdirs(String path) throws IOException {
        return false;
    }

    @Override
    public DataInputStream open(String path) throws IOException {
        return null;
    }

    @Override
    public DataOutputStream create(String path, boolean overwrite, int bufferSize, short replication) throws IOException {
        return null;
    }

    @Override
    public DataOutputStream create(String path, boolean overwrite, int bufferSize, short replication, long blockSize) throws IOException {
        return null;
    }

    @Override
    public boolean delete(String path, boolean recursive) throws IOException {
        return false;
    }

    @Override
    public boolean supportDirectCopy() {
        return false;
    }

    @Override
    public boolean directCopy(String src, String dest) throws IOException {
        return false;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public boolean exists(String path) throws IOException {
        return false;
    }

    @Override
    public boolean isFile(String path) throws IOException {
        return false;
    }

    @Override
    public boolean isDirectory(String path) throws IOException {
        return false;
    }
}
