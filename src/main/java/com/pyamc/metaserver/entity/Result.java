package com.pyamc.metaserver.entity;

public class Result {
    public String code;
    public String message;

    public static Result Success() {
        return new Result();
    }

    public static Result Fail() {
        return new Result();
    }
}
