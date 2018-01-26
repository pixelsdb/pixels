package cn.edu.ruc.iir.pixels.metadata.dao;

import cn.edu.ruc.iir.pixels.metadata.util.DBUtils;

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

    List<T> loadT(String sql);

    void update(String sql);

    List<T> find(T o);

    List<T> loadT(T o);

    void update(T o);

}
