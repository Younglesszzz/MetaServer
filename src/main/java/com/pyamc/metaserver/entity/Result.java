package com.pyamc.metaserver.entity;

public class Result {
    public int code;
    public String message;

    public static Result Success() {
        return new Result(0, "success");
    }

    public static Result Fail() {
        return new Result(500, "fail");
    }

    public Result() {
    }

    public Result(int code, String message) {
        this.code = code;
        this.message = message;
    }
}
