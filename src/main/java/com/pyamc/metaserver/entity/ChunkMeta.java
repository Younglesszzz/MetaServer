package com.pyamc.metaserver.entity;

import java.util.List;

public class ChunkMeta {
    private String chunkKey;
    private List<INodeSnapShot> inodes;

    public ChunkMeta(String chunkKey, List<INodeSnapShot> inodes) {
        this.chunkKey = chunkKey;
        this.inodes = inodes;
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
