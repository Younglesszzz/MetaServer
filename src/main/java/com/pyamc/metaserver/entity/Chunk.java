package com.pyamc.metaserver.entity;

public class Chunk {
    protected String chunkKey;
    private byte[] buffer;
    private String checkSum;
    private int size;


    public Chunk() {
    }

    public Chunk(byte[] buffer, String checkSum) {
        this.buffer = buffer;
        this.checkSum = checkSum;
    }

    public Chunk(String chunkId, byte[] buffer, String checkSum) {
        this.chunkKey = chunkId;
        this.buffer = buffer;
        this.checkSum = checkSum;
    }

    public Chunk(String chunkKey, byte[] buffer, String checkSum, int size) {
        this.chunkKey = chunkKey;
        this.buffer = buffer;
        this.checkSum = checkSum;
        this.size = size;
    }


    public String getChunkKey() {
        return chunkKey;
    }

    public void setChunkKey(String chunkKey) {
        this.chunkKey = chunkKey;
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

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }
}
