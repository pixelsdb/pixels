package cn.edu.ruc.iir.pixels.common.serialize;

import com.esotericsoftware.kryo.Kryo;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.common.serialize
 * @ClassName: ThreadLocalKryoFactory
 * @Description: 为每个线程创建一个Kryo对象
 * @author: tao
 * @date: Create in 2018-09-18 19:47
 **/
public class ThreadLocalKryoFactory extends KryoFactory {

    private final ThreadLocal<Kryo> holder = new ThreadLocal<Kryo>() {
        @Override
        protected Kryo initialValue() {
            return createKryo();
        }
    };

    public Kryo getKryo() {
        return holder.get();
    }
}
