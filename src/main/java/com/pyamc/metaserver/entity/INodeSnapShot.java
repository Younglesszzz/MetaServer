package com.pyamc.metaserver.entity;

public class INodeSnapShot {
    String nodeKey;
    long offset;
    long status;

    public INodeSnapShot(String nodeKey, long offset) {
        this.nodeKey = nodeKey;
        this.offset = offset;
    }

    public INodeSnapShot(String nodeKey, long offset, long status) {
        this.nodeKey = nodeKey;
        this.offset = offset;
        this.status = status;
    }

    public String getNodeKey() {
        return nodeKey;
    }

    public void setNodeKey(String nodeKey) {
        this.nodeKey = nodeKey;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public long getStatus() {
        return status;
    }

    public void setStatus(long status) {
        this.status = status;
    }
}
