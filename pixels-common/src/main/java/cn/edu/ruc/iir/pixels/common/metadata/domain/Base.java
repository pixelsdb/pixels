package cn.edu.ruc.iir.pixels.common.metadata.domain;

import java.io.Serializable;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.common.metadata.domain
 * @ClassName: Base
 * @Description:
 * @author: tao
 * @date: Create in 2018-09-18 15:07
 **/
public class Base implements Serializable {
    private static final long serialVersionUID = -1320564794031870596L;
    private int id;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    @Override
    public int hashCode()
    {
        return this.id;
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
