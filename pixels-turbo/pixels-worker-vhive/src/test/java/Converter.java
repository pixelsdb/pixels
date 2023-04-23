import com.alibaba.fastjson.JSON;

import static org.junit.Assert.assertEquals;

public class Converter<T> {
    final Class<T> typeParameterClass;

    public Converter(Class<T> typeParameterClass) {
        this.typeParameterClass = typeParameterClass;
    }

    public void executeTest(T input) {
        String json = JSON.toJSONString(input);
        T converted = JSON.parseObject(json, typeParameterClass);
        assertEquals(converted, input);
    }
}
