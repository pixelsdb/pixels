package io.pixelsdb.pixels.common.metadata.domain;

import java.io.Serializable;


public class Base implements Serializable
{
    private long id;

    public long getId()
    {
        return id;
    }

    public void setId(long id)
    {
        this.id = id;
    }

    @Override
    public int hashCode()
    {
        return (int) this.id;
    }

    @Override
    public boolean equals(Object o)
    {
        if (o instanceof Base)
        {
            return this.id == ((Base) o).id;
        }
        return false;
    }
}
