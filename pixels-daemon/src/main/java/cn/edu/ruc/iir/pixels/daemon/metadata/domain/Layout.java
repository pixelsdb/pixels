package cn.edu.ruc.iir.pixels.daemon.metadata.domain;

import java.io.Serializable;
import java.math.BigInteger;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.daemon.metadata.domain
 * @ClassName: Layout
 * @Description: layouts info
 * @author: tao
 * @date: Create in 2018-01-26 10:10
 **/
public class Layout implements Serializable {

    private int layId;
    private int layVerion;
    private BigInteger layCreateAt;
    private Integer layActive;
    private Integer layEnabled;
    private BigInteger layEnabledAt;
    private String layInitOrder;
    private String layInitPath;
    private String layCompact;
    private String layCompactPath;
    private String laySplit;
    private Table lt;

    public Layout() {
    }

    public Layout(int layId, int layVerion, BigInteger layCreateAt, Integer layActive, Integer layEnabled, BigInteger layEnabledAt, String layInitOrder, String layInitPath, String layCompact, String layCompactPath, String laySplit, Table lt) {
        this.layId = layId;
        this.layVerion = layVerion;
        this.layCreateAt = layCreateAt;
        this.layActive = layActive;
        this.layEnabled = layEnabled;
        this.layEnabledAt = layEnabledAt;
        this.layInitOrder = layInitOrder;
        this.layInitPath = layInitPath;
        this.layCompact = layCompact;
        this.layCompactPath = layCompactPath;
        this.laySplit = laySplit;
        this.lt = lt;
    }

    public int getLayId() {
        return layId;
    }

    public void setLayId(int layId) {
        this.layId = layId;
    }

    public int getLayVerion() {
        return layVerion;
    }

    public void setLayVerion(int layVerion) {
        this.layVerion = layVerion;
    }

    public BigInteger getLayCreateAt() {
        return layCreateAt;
    }

    public void setLayCreateAt(BigInteger layCreateAt) {
        this.layCreateAt = layCreateAt;
    }

    public Integer getLayActive() {
        return layActive;
    }

    public void setLayActive(Integer layActive) {
        this.layActive = layActive;
    }

    public Integer getLayEnabled() {
        return layEnabled;
    }

    public void setLayEnabled(Integer layEnabled) {
        this.layEnabled = layEnabled;
    }

    public BigInteger getLayEnabledAt() {
        return layEnabledAt;
    }

    public void setLayEnabledAt(BigInteger layEnabledAt) {
        this.layEnabledAt = layEnabledAt;
    }

    public String getLayInitOrder() {
        return layInitOrder;
    }

    public void setLayInitOrder(String layInitOrder) {
        this.layInitOrder = layInitOrder;
    }

    public String getLayInitPath() {
        return layInitPath;
    }

    public void setLayInitPath(String layInitPath) {
        this.layInitPath = layInitPath;
    }

    public String getLayCompact() {
        return layCompact;
    }

    public void setLayCompact(String layCompact) {
        this.layCompact = layCompact;
    }

    public String getLayCompactPath() {
        return layCompactPath;
    }

    public void setLayCompactPath(String layCompactPath) {
        this.layCompactPath = layCompactPath;
    }

    public String getLaySplit() {
        return laySplit;
    }

    public void setLaySplit(String laySplit) {
        this.laySplit = laySplit;
    }

    public Table getLt() {
        return lt;
    }

    public void setLt(Table lt) {
        this.lt = lt;
    }

    @Override
    public String toString() {
        return "Layout{" +
                "layId=" + layId +
                ", layVerion=" + layVerion +
                ", layCreateAt=" + layCreateAt +
                ", layActive=" + layActive +
                ", layEnabled=" + layEnabled +
                ", layEnabledAt=" + layEnabledAt +
                ", layInitOrder='" + layInitOrder + '\'' +
                ", layInitPath='" + layInitPath + '\'' +
                ", layCompact='" + layCompact + '\'' +
                ", layCompactPath='" + layCompactPath + '\'' +
                ", laySplit='" + laySplit + '\'' +
                ", lt=" + lt +
                '}';
    }
}