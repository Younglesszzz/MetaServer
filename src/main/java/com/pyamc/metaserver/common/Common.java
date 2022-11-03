package com.pyamc.metaserver.common;

public class Common {
    private static final int ChunkSize = 64 * 1024 * 1024;
    private static final int CheckSumSize = 32;
    private static final int FileBytesSize = ChunkSize - CheckSumSize;
}
