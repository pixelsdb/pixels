package cn.edu.ruc.iir.pixels.common.metadata;

import java.io.Serializable;

public class ResParams implements Serializable
{

    private String action;
    private String result;

    public ResParams(String action) {
        this.action = action;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }

    @Override
    public String toString() {
        return "ResParams{" +
                "action='" + action + '\'' +
                ", result='" + result + '\'' +
                '}';
    }
}
