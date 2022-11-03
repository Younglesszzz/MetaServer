package com.pyamc.metaserver.service;

import com.pyamc.metaserver.entity.*;
import com.pyamc.metaserver.util.FileUtil;
import java.lang.String;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.TxnResponse;
import io.etcd.jetcd.options.GetOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.Resource;
import javax.xml.crypto.Data;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

enum FileStatus {
    Initial("0"),  Done("1");
    private final String status;
    FileStatus (String status) {
        this.status = status;
    }
    public String getStatus() {
        return this.status;
    }
}

enum ChunkStatus {
    Initial("0"),  Done("3");
    private final String status;
    ChunkStatus (String status) {
        this.status = status;
    }
    public String getStatus() {
        return this.status;
    }
}

@Service
public class FileService {
    @Resource
    EtcdService etcdService;
    private Logger logger = LoggerFactory.getLogger(FileService.class);
    private static final int ChunkSize = 64 * 1024 * 1024;
    private static final int CheckSumSize = 32;
    private static final int FileBytesSize = ChunkSize - CheckSumSize;
    private static final int retryTimes = 3;
    public Result process(MultipartFile uploadFile) {
        try {
            byte[] bytes = uploadFile.getBytes();
            String fileKey = FileUtil.getMD5sum(bytes);
            if (fileKey.length() == 0) {
                logger.warn("Process#File Key Is Empty");
                return null;
            }
            // 先查元数据
            String fileStatus = getFileStatus(fileKey);
            if (fileStatus.equals(FileStatus.Done.getStatus())) {
                return Result.Success();
            }
            if (fileStatus.equals("")) {
                etcdService.put(getStatusKey(fileKey), FileStatus.Initial.getStatus());
            }
            // 进行分块
            List<ChunkInfo> chunks = buildChunks(fileKey, bytes);
            // file -> chunks
            etcdService.put(getFile2ChunksKey(fileKey), buildFile2ChunksVal(chunks));
            for (int i = 0; i < chunks.size(); i++) {
                List<DataNode> nodes =  calcDataNodes();
                ChunkInfo c = chunks.get(i);
                // chunk -> inodes
                etcdService.put(getChunk2NodesKey(c.getChunk().getChunkId()), buildChunk2NodesVal(nodes));
                c.setInodes(nodes);
                // 写入分块初始状态
                etcdService.put(getStatusKey(c.getChunk().getChunkId()), FileStatus.Initial.getStatus());
                // 分块发送
                sendChunk2DataNode(c, c.getInodes());
                // 分块状态变更
                etcdService.put(getStatusKey(c.getChunk().getChunkId()), FileStatus.Done.getStatus());
            }
            // 文件状态变更
            etcdService.put(getStatusKey(fileKey), FileStatus.Done.getStatus());
        } catch (IOException | ExecutionException | InterruptedException e) {
            e.printStackTrace();
            return Result.Fail();
        }
        return Result.Success();
    }

    private String buildChunk2NodesVal(List<DataNode> nodes) {
        StringBuilder sb = new StringBuilder();
        for (DataNode node : nodes) {
            sb.append(node.getKey()).append(";");
        }
        return sb.toString();
    }

    private String getChunk2NodesKey(String chunkId) {
        return "CHUNK2NODES_" + chunkId;
    }

    private String buildFile2ChunksVal(List<ChunkInfo> chunks) {
        StringBuilder sb = new StringBuilder();
        for (ChunkInfo chunk : chunks) {
            sb.append(chunk.getChunk().getChunkId()).append(";");
        }
        return sb.toString();
    }

    private String getFile2ChunksKey(String fileKey) {
        return "FILE2CHUNK_" + fileKey;
    }

    private void sendChunk2DataNode(ChunkInfo c, List<DataNode> inodes) throws ExecutionException, InterruptedException {
        StringBuilder inodeSnapShot = new StringBuilder();
        // 预占空间
        for (DataNode inode : inodes) {
            String checkPointKey = getNodeInfoKey(inode.getKey());
            int i = 0;
            while (true) {
                if (i == retryTimes) {
                    logger.warn("SendChunk2DataNode#ETCD CAS RETRY REACH LIMIT");
                    return;
                }
                String expect = etcdService.syncGetValue(checkPointKey);
                if (expect == null) {
                    logger.warn("SendChunk2DataNode#Sync Get Value Failed");
                    return;
                }
                if (etcdService.syncCas(checkPointKey, expect, expect + ChunkSize)) {
                    inodeSnapShot.append(inode.getKey()).append(":").append(expect).append(";");
                    break;
                }
                i++;
            }
        }
        etcdService.put(getChunk2NodesKey(c.getChunk().getChunkId()), inodeSnapShot.substring(0, inodeSnapShot.length() - 1));
        // 实际发送

    }

    private String getNodeInfoKey(String nodeName) {
        return "NODEINFO_" + nodeName;
    }

    // 根据资源利用率选择节点
    private List<DataNode> calcDataNodes() {
        GetResponse res = null;
        try {
             res = etcdService.getWithOption("", GetOption.newBuilder().
                    isPrefix(true).
                    withLimit(3).
                    withSortField(GetOption.SortTarget.VALUE).
                    withSortOrder(GetOption.SortOrder.ASCEND).
                    build()
            ).get();
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }

        return res == null ? null : buildDataNodes(res.getKvs());
    }

    private List<DataNode> buildDataNodes(List<KeyValue> kvs) {
        if (kvs == null || kvs.size() == 0) {
            return null;
        }
        List<DataNode> res = new ArrayList<>(3);
        for (KeyValue kv : kvs) {
            String value = kv.getValue().toString();
            res.add(new DataNode(kv.getKey().toString(), kv.getValue().toString()));
        }
        return res;
    }

    private String getFileStatus(String fileKey) throws ExecutionException, InterruptedException {
        GetResponse response = etcdService.get(getStatusKey(fileKey)).get();
        if (response.getKvs().size() > 0) {
            KeyValue kv = response.getKvs().get(0);
            return kv.getValue().toString();
        }
        return "";
    }

    private List<ChunkInfo> buildChunks(String fileKey, byte[] bytes) {
        List<byte[]> bytesList = split2BytesList(bytes);
        List<ChunkInfo> chunks = new ArrayList<>(bytesList.size());
        for (int i = 0; i < bytesList.size(); i++) {
            byte[] b = bytesList.get(i);
            String md5Code = FileUtil.getMD5sum(b);
            String chunkKey = getChunkKey(fileKey, i);
            Chunk c = new Chunk(chunkKey, b, md5Code);
            ChunkInfo chunkInfo = new ChunkInfo(c);
            chunks.add(chunkInfo);
        }
        return chunks;
    }

    private List<byte[]> split2BytesList(byte[] bytes) {
        int len = (int) Math.ceil((double) bytes.length / (double) FileBytesSize);
        List<byte[]> res = new ArrayList<>(len);
        int from = 0, to = 0;
        for (int i = 0; i < len; i++) {
            from = i * FileBytesSize;
            to = from + FileBytesSize;
            if (to > bytes.length) {
                to = bytes.length;
            }
            res.add(Arrays.copyOfRange(bytes, from, to));
        }
        return res;
    }

    public void merge() {

    }

    // status 0: 元数据填充 1: 数据写入完成
    public String getStatusKey(String fileKey) {
        return "STATUS_" + fileKey;
    }

    private String getChunkKey(String fileKey, int seq) {
        return fileKey + "_" + seq;
    }

    public File buildNewFile(String fileKey ) {
        return new File();
    }


}
