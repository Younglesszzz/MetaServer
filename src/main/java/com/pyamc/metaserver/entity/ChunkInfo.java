package com.pyamc.metaserver.entity;

import java.util.List;

public class ChunkInfo {
    private List<DataNode> inodes;
    private Chunk chunk;

    public ChunkInfo(Chunk chunk) {
        this.chunk = chunk;
    }

    public List<DataNode> getInodes() {
        return inodes;
    }

    public void setInodes(List<DataNode> inodes) {
        this.inodes = inodes;
    }

    public Chunk getChunk() {
        return chunk;
    }

    public void setChunk(Chunk chunk) {
        this.chunk = chunk;
    }
}
