package com.pyamc.metaserver.service;

import com.alibaba.fastjson.JSON;
import com.pyamc.metaserver.entity.*;
import com.pyamc.metaserver.util.FileUtil;
import java.lang.String;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.options.GetOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
    private static Map<String, FileMeta> lockPool = new ConcurrentHashMap<>();
    public Result process(MultipartFile uploadFile) {
        String fileKey = null;
        try {
            byte[] bytes = uploadFile.getBytes();
            fileKey = getMd5Code(bytes);
            if (fileKey.isEmpty()) {
                return Result.Fail();
            }
            // 校验文件状态
            FileMeta fileMeta = getFileMeta(fileKey);
            if (fileMeta != null && FileStatus.Done.getStatus().equals(fileMeta.getFileStatus())) {
                return Result.Success();
            }
            // todo 后续支持续传
            else if (fileMeta != null && FileStatus.Initial.getStatus().equals(fileMeta.getFileStatus())) {
                return Result.Fail();
            }
            List<Chunk> chunks = buildChunks(fileKey, bytes);
            // todo 原子性问题 etcd 无法解决 需要加锁 单机/分布式
            putFileMeta(fileKey, chunks, uploadFile);
            for (int i = 0; i < chunks.size(); i++) {
                List<DataNode> nodes =  calcDataNodes();
                if (nodes == null || nodes.size() < 3) {
                    return Result.Fail();
                }
                Chunk c = chunks.get(i);
                // 分块发送
                sendChunk2DataNode(c, nodes);
            }
            // 文件状态变更
        } catch (IOException | ExecutionException | InterruptedException e) {
            e.printStackTrace();
            return Result.Fail();
        } finally {
            if (fileKey != null) {
                lockPool.remove(fileKey);
            }
        }
        return Result.Success();
    }

    private FileMeta getFileMeta(String fileKey) throws ExecutionException, InterruptedException {
        String value = etcdService.syncGetValue(fileKey);
        if (value.isEmpty()) {
            return null;
        }
        return (FileMeta) JSON.parse(value);
    }

    private String getMd5Code(byte[] bytes) throws IOException, ExecutionException, InterruptedException {
        String fileKey = FileUtil.getMD5sum(bytes);
        if (fileKey.length() == 0) {
            logger.warn("Process#File Key Is Empty");
            return "";
        }
        return fileKey;
    }


    private void putFileMeta(String fileKey, List<Chunk> chunks, MultipartFile uploadFile) {
        etcdService.put(getFileMetaKey(fileKey), buildFileMeta(fileKey, chunks, uploadFile));
    }

    private String buildChunk2NodesVal(List<DataNode> nodes) {
        StringBuilder sb = new StringBuilder();
        for (DataNode node : nodes) {
            sb.append(node.getKey()).append(";");
        }
        return sb.toString();
    }

    private String getChunkMetaKey(String chunkId) {
        return "CHUNKINFO_" + chunkId;
    }

    private String buildFileMeta(String fileKey, List<Chunk> chunks, MultipartFile uploadFile) {
        List<String> chunkKeys = new ArrayList<>(chunks.size());
        for (Chunk chunk : chunks) {
            chunkKeys.add(chunk.getChunkKey());
        }
        FileMeta fileMeta = new FileMeta(uploadFile.getOriginalFilename(),
                fileKey, uploadFile.getSize(), uploadFile.getContentType(), chunkKeys);
        return JSON.toJSONString(fileMeta);
    }

    private String getFileMetaKey(String fileKey) {
        return "FILE_KEY" + fileKey;
    }

    private void sendChunk2DataNode(Chunk c, List<DataNode> inodes) throws ExecutionException, InterruptedException {
        List<INodeSnapShot> snapShots = new ArrayList<>(inodes.size());
        // 预占DataNode空间
        for (DataNode inode : inodes) {
            int i = 0;
            while (true) {
                if (i == retryTimes) {
                    logger.warn("SendChunk2DataNode#ETCD CAS RETRY REACH LIMIT");
                    return;
                }
                DataNode node = GetNodeInfo(inode.getKey());
                if (node == null) {
                    logger.warn("SendChunk2DataNode#Node Not Exist");
                    return;
                }
                DataNode copy = new DataNode();
                BeanUtils.copyProperties(node, copy);
                copy.setCheckpoint(copy.getCheckpoint() + ChunkSize);
                if (etcdService.syncCas(getNodeInfoKey(inode.getKey()),
                        JSON.toJSONString(node), JSON.toJSONString(copy))) {
                    snapShots.add(new INodeSnapShot(inode.getKey(), node.getCheckpoint(), 0));
                    break;
                }
                i++;
            }
        }
        ChunkMeta cm = new ChunkMeta(c.getChunkKey(), snapShots);
        etcdService.put(getChunkMetaKey(c.getChunkKey()), JSON.toJSONString(cm));
        // 实际发送
        for (int i = 0; i < snapShots.size(); i++) {
//            HttpSendChunk();
            cm.getInodes().get(i).setStatus(1);
        }
        // update chunk meta
        etcdService.put(getChunkMetaKey(c.getChunkKey()), JSON.toJSONString(cm));
    }

    private DataNode GetNodeInfo(String key) throws ExecutionException, InterruptedException {
        String value = etcdService.syncGetValue(getNodeInfoKey(key));
        return value.isEmpty() ? null : (DataNode) JSON.parse(value);
    }

    private String getNodeInfoKey(String nodeName) {
        return "NODEINFO_" + nodeName;
    }

    // 根据资源利用率选择节点
    private List<DataNode> calcDataNodes() {
        GetResponse res = null;
        try {
             res = etcdService.getWithOption("DATANODE_", GetOption.newBuilder().
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

    private List<Chunk> buildChunks(String fileKey, byte[] bytes) {
        List<byte[]> bytesList = split2BytesList(bytes);
        List<Chunk> chunks = new ArrayList<>(bytesList.size());
        for (int i = 0; i < bytesList.size(); i++) {
            byte[] b = bytesList.get(i);
            String md5Code = FileUtil.getMD5sum(b);
            String chunkKey = getChunkKey(fileKey, i);
            Chunk c = new Chunk(chunkKey, b, md5Code);
            chunks.add(c);
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
