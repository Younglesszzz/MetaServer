package com.pyamc.metaserver.entity;

public class Chunk {
    protected String chunkId;
    private byte[] buffer;
    private String checkSum;

    public Chunk() {
    }

    public Chunk(byte[] buffer, String checkSum) {
        this.buffer = buffer;
        this.checkSum = checkSum;
    }

    public Chunk(String chunkId, byte[] buffer, String checkSum) {
        this.chunkId = chunkId;
        this.buffer = buffer;
        this.checkSum = checkSum;
    }

    public String getChunkId() {
        return chunkId;
    }

    public void setChunkId(String chunkId) {
        this.chunkId = chunkId;
    }

    public byte[] getBuffer() {
        return buffer;
    }

    public void setBuffer(byte[] buffer) {
        this.buffer = buffer;
    }

    public String getCheckSum() {
        return checkSum;
    }

    public void setCheckSum(String checkSum) {
        this.checkSum = checkSum;
    }
}
