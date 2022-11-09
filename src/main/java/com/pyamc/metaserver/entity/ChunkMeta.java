package com.pyamc.metaserver.entity;

import java.util.List;

public class ChunkMeta {
    private String chunkKey;
    private int size;
    private int seq;
    private List<INodeSnapShot> inodes;

    public ChunkMeta(String chunkKey, List<INodeSnapShot> inodes) {
        this.chunkKey = chunkKey;
        this.inodes = inodes;
    }

    public ChunkMeta(String chunkKey, int size, int seq, List<INodeSnapShot> inodes) {
        this.chunkKey = chunkKey;
        this.size = size;
        this.seq = seq;
        this.inodes = inodes;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public int getSeq() {
        return seq;
    }

    public void setSeq(int seq) {
        this.seq = seq;
    }

    public String getChunkKey() {
        return chunkKey;
    }

    public void setChunkKey(String chunkKey) {
        this.chunkKey = chunkKey;
    }

    public List<INodeSnapShot> getInodes() {
        return inodes;
    }

    public void setInodes(List<INodeSnapShot> inodes) {
        this.inodes = inodes;
    }

}
