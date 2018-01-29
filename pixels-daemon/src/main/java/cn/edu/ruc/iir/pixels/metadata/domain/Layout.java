package cn.edu.ruc.iir.pixels.metadata.domain;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Set;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.metadata.domain
 * @ClassName: Layout
 * @Description: layouts info
 * @author: tao
 * @date: Create in 2018-01-26 10:10
 **/
public class Layout implements Serializable {

    private int layId;
    private String layVer;
    private String layVal;
    private Timestamp layTime;
    private Table lt;
    private Set<Catalog> layCatalogs = new HashSet(0);

    public Layout() {
    }

    public Layout(int layId, String layVer, String layVal, Timestamp layTime, Table lt, Set<Catalog> layCatalogs) {
        this.layId = layId;
        this.layVer = layVer;
        this.layVal = layVal;
        this.layTime = layTime;
        this.lt = lt;
        this.layCatalogs = layCatalogs;
    }

    public Timestamp getLayTime() {
        return layTime;
    }

    public void setLayTime(Timestamp layTime) {
        this.layTime = layTime;
    }

    public int getLayId() {
        return layId;
    }

    public void setLayId(int layId) {
        this.layId = layId;
    }

    public String getLayVer() {
        return layVer;
    }

    public void setLayVer(String layVer) {
        this.layVer = layVer;
    }

    public String getLayVal() {
        return layVal;
    }

    public void setLayVal(String layVal) {
        this.layVal = layVal;
    }

    public Table getLt() {
        return lt;
    }

    public void setLt(Table lt) {
        this.lt = lt;
    }

    public Set<Catalog> getLayCatalogs() {
        return layCatalogs;
    }

    public void setLayCatalogs(Set<Catalog> layCatalogs) {
        this.layCatalogs = layCatalogs;
    }
}