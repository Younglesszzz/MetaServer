package com.pyamc.metaserver.entity;

import java.util.List;

public class FileMeta {
    private String fileName;
    private String fileKey;
    private long fileSize;
    private String contentType;
    private long fileStatus;
    private List<String> chunkKeys;

    public FileMeta(String fileName, String fileKey, long fileSize, String contentType, List<String> chunkKeys) {
        this.fileName = fileName;
        this.fileKey = fileKey;
        this.fileSize = fileSize;
        this.contentType = contentType;
        this.chunkKeys = chunkKeys;
        this.fileStatus = 0;
    }

    public long getFileStatus() {
        return fileStatus;
    }

    public void setFileStatus(long fileStatus) {
        this.fileStatus = fileStatus;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getFileKey() {
        return fileKey;
    }

    public void setFileKey(String fileKey) {
        this.fileKey = fileKey;
    }

    public long getFileSize() {
        return fileSize;
    }

    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public List<String> getChunkKeys() {
        return chunkKeys;
    }

    public void setChunkKeys(List<String> chunkKeys) {
        this.chunkKeys = chunkKeys;
    }
}
