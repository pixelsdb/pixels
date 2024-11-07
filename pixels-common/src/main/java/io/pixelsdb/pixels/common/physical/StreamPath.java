package io.pixelsdb.pixels.common.physical;

import static java.util.Objects.requireNonNull;

public class StreamPath
{
    private String host;
    private int port;
    public boolean valid = false;

    public StreamPath(String path)
    {
        requireNonNull(path);
        if (path.contains(":///"))
        {
            path = path.substring(path.indexOf(":///") + 4);
        }
        int colon = path.indexOf(':');
        if (colon > 0)
        {
            host = path.substring(0, colon);
            port = Integer.parseInt(path.substring(colon + 1));
            this.valid = true;
        }
    }

    public String getHostName()
    {
        return host;
    }

    public int getPort()
    {
        return port;
    }

}
