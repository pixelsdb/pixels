package cn.edu.ruc.iir.pixels.daemon.metadata.domain;

import java.io.Serializable;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.daemon.metadata.domain
 * @ClassName: Catalog
 * @Description: Catalogs info
 * @author: tao
 * @date: Create in 2018-01-26 10:10
 **/
public class Catalog implements Serializable {

    private int cataId;
    private String cataPath;
    private Layout cl;

    public Catalog() {
    }

    public Catalog(int cataId, String cataPath, Layout cl) {
        this.cataId = cataId;
        this.cataPath = cataPath;
        this.cl = cl;
    }

    public int getCataId() {
        return cataId;
    }

    public void setCataId(int cataId) {
        this.cataId = cataId;
    }

    public String getCataPath() {
        return cataPath;
    }

    public void setCataPath(String cataPath) {
        this.cataPath = cataPath;
    }

    public Layout getCl() {
        return cl;
    }

    public void setCl(Layout cl) {
        this.cl = cl;
    }

    @Override
    public String toString() {
        return "Catalog{" +
                "cataId=" + cataId +
                ", cataPath='" + cataPath + '\'' +
                ", cl=" + cl +
                '}';
    }
}
