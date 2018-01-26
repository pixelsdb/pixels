package cn.edu.ruc.iir.pixels.metadata.domain;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.metadata.domain
 * @ClassName: Table
 * @Description: tbls info
 * @author: tao
 * @date: Create in 2018-01-26 10:10
 **/
public class Table implements Serializable {
    private int tblId;
    private String tblName;
    private String tblType;
    private Schema ts;
    private Set<Column> tblCols = new HashSet(0);
    private Set<Layout> tblLays = new HashSet(0);

    public Table() {
    }

    public Table(int tblId, String tblName, String tblType, Schema ts, Set<Column> tblCols, Set<Layout> tblLays) {
        this.tblId = tblId;
        this.tblName = tblName;
        this.tblType = tblType;
        this.ts = ts;
        this.tblCols = tblCols;
        this.tblLays = tblLays;
    }

    public int getTblId() {
        return tblId;
    }

    public void setTblId(int tblId) {
        this.tblId = tblId;
    }

    public String getTblName() {
        return tblName;
    }

    public void setTblName(String tblName) {
        this.tblName = tblName;
    }

    public String getTblType() {
        return tblType;
    }

    public void setTblType(String tblType) {
        this.tblType = tblType;
    }

    public Schema getTs() {
        return ts;
    }

    public void setTs(Schema ts) {
        this.ts = ts;
    }

    public Set<Column> getTblCols() {
        return tblCols;
    }

    public void setTblCols(Set<Column> tblCols) {
        this.tblCols = tblCols;
    }

    public Set<Layout> getTblLays() {
        return tblLays;
    }

    public void setTblLays(Set<Layout> tblLays) {
        this.tblLays = tblLays;
    }
}
