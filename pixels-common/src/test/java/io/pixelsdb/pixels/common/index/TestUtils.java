package io.pixelsdb.pixels.common.index;
import java.lang.reflect.Field;

public class TestUtils {

    // 设置私有字段
    public static void setPrivateField(Object target, String fieldName, Object value) {
        try {
            Field field = target.getClass().getDeclaredField(fieldName);
            field.setAccessible(true); // 允许访问私有字段
            field.set(target, value);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("Failed to set private field", e);
        }
    }

    // 获取私有字段
    public static <T> T getPrivateField(Object target, String fieldName) {
        try {
            Field field = target.getClass().getDeclaredField(fieldName);
            field.setAccessible(true); // 允许访问私有字段
            return (T) field.get(target);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("Failed to get private field", e);
        }
    }
}
