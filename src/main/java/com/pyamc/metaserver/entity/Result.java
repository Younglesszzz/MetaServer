package com.pyamc.metaserver.entity;

public class Result {
    public String code;
    public String message;
    public Object data;

    public Result(String code, String message, Object data) {
        this.code = code;
        this.message = message;
        this.data = data;
    }

    public Result(String code, String message) {
        this.code = code;
        this.message = message;
    }

    public static Result Success() {
        return new Result("0", "success", null);
    }

    public static Result Fail() {
        return new Result("500", "Fail", null);
    }

    public static Result Success(Object data) {
        return new Result("0", "success", data);
    }

    public static Result Fail(Object data) {
        return new Result("500", "Fail", data);
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }

}

