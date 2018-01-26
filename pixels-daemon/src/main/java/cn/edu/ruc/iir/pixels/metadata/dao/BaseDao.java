package cn.edu.ruc.iir.pixels.metadata.dao;

import cn.edu.ruc.iir.pixels.common.DBUtils;

import java.io.Serializable;
import java.util.List;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.metadata.dao
 * @ClassName: BaseDao
 * @Description: baseDao
 * @author: tao
 * @date: Create in 2018-01-26 10:44
 **/
public interface BaseDao<T> {

    DBUtils db = DBUtils.Instance();

    T get(Serializable id);

    List<T> find(String sql);

    List<T> loadT(String sql, String[] params);

    boolean update(String sql, String[] params);

    List<T> find(T o);

    List<T> loadT(T o);

    boolean update(T o, String[] params);

}
