package cn.edu.ruc.iir.pixels.metadata.domain;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.metadata.domain
 * @ClassName: Schema
 * @Description: DBS info
 * @author: tao
 * @date: Create in 2018-01-26 10:09
 **/
public class Schema implements Serializable {
    private int scheId;
    private String schName;
    private String schDesc;

    private Set<Table> schTbls = new HashSet(0);

    public Schema() {
    }

    public Schema(int scheId, String schName, String schDesc, Set<Table> schTbls) {
        this.scheId = scheId;
        this.schName = schName;
        this.schDesc = schDesc;
        this.schTbls = schTbls;
    }

    public int getScheId() {
        return scheId;
    }

    public void setScheId(int scheId) {
        this.scheId = scheId;
    }

    public String getSchName() {
        return schName;
    }

    public void setSchName(String schName) {
        this.schName = schName;
    }

    public String getSchDesc() {
        return schDesc;
    }

    public void setSchDesc(String schDesc) {
        this.schDesc = schDesc;
    }

    public Set<Table> getSchTbls() {
        return schTbls;
    }

    public void setSchTbls(Set<Table> schTbls) {
        this.schTbls = schTbls;
    }
}
