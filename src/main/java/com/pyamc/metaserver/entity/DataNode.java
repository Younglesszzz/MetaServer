package com.pyamc.metaserver.entity;

public class DataNode implements Cloneable {
    private String key;
    private String url;
    private long checkpoint;

    public DataNode() {
    }

    public DataNode(String key, String url) {
        this.key = key;
        this.url = url;
        this.checkpoint = 0;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public long getCheckpoint() {
        return checkpoint;
    }

    public void setCheckpoint(long checkpoint) {
        this.checkpoint = checkpoint;
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}
