package com.pyamc.metaserver.entity;

import java.util.Objects;

public class FileLock {
    private String fileKey;

    public FileLock(String fileKey) {
        this.fileKey = fileKey;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FileLock fileLock = (FileLock) o;
        return Objects.equals(fileKey, fileLock.fileKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fileKey);
    }
}
