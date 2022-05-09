package io.pixelsdb.pixels.executor;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.implementation.FixedValue;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.matcher.ElementMatchers;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author hank
 * @date 09/05/2022
 */
public class TestByteBuddy
{
    @Test
    public void test() throws InstantiationException, IllegalAccessException
    {
        Class<?> dynamicType = new ByteBuddy()
                .subclass(Object.class)
                .method(ElementMatchers.named("toString"))
                .intercept(FixedValue.value("Hello World!"))
                .make()
                .load(getClass().getClassLoader())
                .getLoaded();
        assertThat(dynamicType.newInstance().toString(), is("Hello World!"));
    }

    public class GreetingInterceptor {
        public Object greet(Object argument) {
            return "Hello from " + argument;
        }
    }

    @Test
    public void test1() throws InstantiationException, IllegalAccessException
    {
        Class<? extends java.util.function.Function> dynamicType = new ByteBuddy()
                .subclass(java.util.function.Function.class)
                .method(ElementMatchers.named("apply"))
                .intercept(MethodDelegation.to(new GreetingInterceptor()))
                .make()
                .load(getClass().getClassLoader())
                .getLoaded();
        assertThat((String) dynamicType.newInstance().apply("Byte Buddy"), is("Hello from Byte Buddy"));
    }
}
