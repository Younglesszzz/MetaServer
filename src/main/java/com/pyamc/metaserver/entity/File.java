package com.pyamc.metaserver.entity;

import java.util.List;

public class File {
    private String fileName;
    private String fileKey;
    private byte[] bytes;
    private List<Chunk> chunks;
}
