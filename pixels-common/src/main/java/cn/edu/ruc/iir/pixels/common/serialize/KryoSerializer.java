package cn.edu.ruc.iir.pixels.common.serialize;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.apache.commons.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.common.serialize
 * @ClassName: KryoSerializer
 * @Description: [self-defined Encoder/Decoder](https://www.cnblogs.com/fanguangdexiaoyuer/p/6131042.html)
 * @author: tao
 * @date: Create in 2018-09-18 19:48
 **/
public class KryoSerializer {
    public static final int HEAD_LENGTH = 4;

    private static final ThreadLocalKryoFactory factory = new ThreadLocalKryoFactory();

    public static void serialize(Object object, ByteBuf out) {
        byte[] body = convertToBytes(object);  //将对象转换为byte
        int dataLength = body.length;  //读取消息的长度
        out.writeInt(dataLength);  //先将消息长度写入，也就是消息头
        out.writeBytes(body);  //消息体中包含我们要发送的数据
    }

    private static byte[] convertToBytes(Object object) {
        Kryo kryo = factory.getKryo();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Output output = new Output(baos);
        kryo.writeClassAndObject(output, object);
        output.flush();

        byte[] b = baos.toByteArray();
        try {
            baos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(output);
            IOUtils.closeQuietly(baos);
        }
        return b;
    }

    public static void deserialize(ByteBuf out, ChannelHandlerContext ctx, List<Object> objects) {
        if (out.readableBytes() < HEAD_LENGTH) {  //这个HEAD_LENGTH是我们用于表示头长度的字节数。  由于Encoder中我们传的是一个int类型的值，所以这里HEAD_LENGTH的值为4.
            return;
        }
        out.markReaderIndex();                  //我们标记一下当前的readIndex的位置
        int dataLength = out.readInt();       // 读取传送过来的消息的长度。ByteBuf 的readInt()方法会让他的readIndex增加4
        if (dataLength < 0) { // 我们读到的消息体长度为0，这是不应该出现的情况，这里出现这情况，关闭连接。
            ctx.close();
        }

        if (out.readableBytes() < dataLength) { //读到的消息体长度如果小于我们传送过来的消息长度，则resetReaderIndex. 这个配合markReaderIndex使用的。把readIndex重置到mark的地方
            out.resetReaderIndex();
            return;
        }

        byte[] body = new byte[dataLength];  //传输正常
        out.readBytes(body);

        Object obj = convertToObject(body);  //将byte数据转化为我们需要的对象
        objects.add(obj);
    }

    private static Object convertToObject(byte[] body) {
        Kryo kryo = factory.getKryo();
        Input input = null;
        ByteArrayInputStream bais = null;
        try {
            bais = new ByteArrayInputStream(body);
            input = new Input(bais);

            return kryo.readClassAndObject(input);
        } catch (KryoException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(input);
            IOUtils.closeQuietly(bais);
        }
        return null;
    }
}
