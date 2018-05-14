package cn.edu.ruc.iir.pixels.daemon.metadata.dao;

import cn.edu.ruc.iir.pixels.common.utils.DBUtils;

import java.io.Serializable;
import java.util.List;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.daemon.metadata.dao
 * @ClassName: BaseDao
 * @Description: baseDao
 * @author: tao
 * @date: Create in 2018-01-26 10:44
 **/
public interface BaseDao<T> {

    DBUtils db = DBUtils.Instance();

    T get(Serializable id);

    List<T> find(String sql);

    List<T> loadAll(String sql, String[] params);

    boolean update(String sql, String[] params);

    List<T> find(T o);

    List<T> loadAll(T o);

    boolean update(T o, String[] params);
}
