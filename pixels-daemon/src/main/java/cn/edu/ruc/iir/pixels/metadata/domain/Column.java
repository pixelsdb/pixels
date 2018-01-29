package cn.edu.ruc.iir.pixels.metadata.domain;

import java.io.Serializable;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.metadata.domain
 * @ClassName: Column
 * @Description: Cols info
 * @author: tao
 * @date: Create in 2018-01-26 10:10
 **/
public class Column implements Serializable {
    private int colId;
    private String colName;
    private String colType;
    private String colLen;
    private String colComment;
    private Table ct;

    public Column() {
    }

    public Column(int colId, String colName, String colType, String colLen, String colComment, Table ct) {
        this.colId = colId;
        this.colName = colName;
        this.colType = colType;
        this.colLen = colLen;
        this.colComment = colComment;
        this.ct = ct;
    }

    public int getColId() {
        return colId;
    }

    public void setColId(int colId) {
        this.colId = colId;
    }

    public String getColName() {
        return colName;
    }

    public void setColName(String colName) {
        this.colName = colName;
    }

    public String getColType() {
        return colType;
    }

    public void setColType(String colType) {
        this.colType = colType;
    }

    public String getColLen() {
        return colLen;
    }

    public void setColLen(String colLen) {
        this.colLen = colLen;
    }

    public String getColComment() {
        return colComment;
    }

    public void setColComment(String colComment) {
        this.colComment = colComment;
    }

    public Table getCt() {
        return ct;
    }

    public void setCt(Table ct) {
        this.ct = ct;
    }
}
