package cn.edu.ruc.iir.pixels.common.metadata;

import java.io.Serializable;

public class Response implements Serializable
{
    private static final long serialVersionUID = 8065670468601076667L;
    private Object result;

    public Response() {
    }

    public Object getResult() {
        return result;
    }

    public void setResult(Object result) {
        this.result = result;
    }

    @Override
    public String toString() {
        return "Response{" +
                "result=" + result +
                '}';
    }
}
