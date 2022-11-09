package com.pyamc.metaserver.exception;

public class BizException extends Exception {
    private String msg;

    public BizException() {
        super();
    }

    public BizException(String message) {
        super(message);
        msg = message;
    }


    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }
}
