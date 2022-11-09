package com.pyamc.metaserver.service;

import com.alibaba.fastjson.JSON;
import com.pyamc.metaserver.entity.*;
import com.pyamc.metaserver.exception.BizException;
import com.pyamc.metaserver.util.FileUtil;

import java.io.ByteArrayOutputStream;
import java.lang.String;

import com.pyamc.metaserver.util.HttpUtil;
import com.pyamc.metaserver.util.NumUtil;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.options.GetOption;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.StringBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

enum FileStatus {
    Initial(0),  Done(1);
    private final long status;
    FileStatus (long status) {
        this.status = status;
    }
    public long getStatus() {
        return this.status;
    }
}

@Service
public class FileService {
    @Resource
    EtcdService etcdService;
    private Logger logger = LoggerFactory.getLogger(FileService.class);
    private static final int ChunkCapacity = 64 * 1024 * 1024;
    private static final int CheckSumBytesNum = 32;
    private static final int ChunkSizeBytesNum = 4;
    private static final int ChunkSeqBytesNum = 4;
    private static final int FileBytesSize = ChunkCapacity - 2 * CheckSumBytesNum - ChunkSizeBytesNum - ChunkSeqBytesNum;
    private static final int retryTimes = 3;
    private static final int replicaFactor = 3;
    public Result process(MultipartFile uploadFile) {
        logger.info("Process#uploadFile: {}", JSON.toJSONString(uploadFile));
        String fileKey = null;
        try {
            byte[] bytes = uploadFile.getBytes();
            fileKey = getMd5Code(bytes);
            if (fileKey.isEmpty()) {
                return Result.Fail();
            }
            // 校验文件状态
            Result checkRes = preCheckFileStatus(fileKey);
            if (checkRes != null) {
                return checkRes;
            }
            List<Chunk> chunks = buildChunks(fileKey, bytes);
            FileMeta fm = buildFileMeta(fileKey, chunks, uploadFile);
            // 初始化文件状态
            etcdService.setNx(getFileMetaKey(fileKey), JSON.toJSONString(fm));
            for (Chunk chunk : chunks) {
                List<DataNode> nodes = calcDataNodes();
                if (nodes == null || nodes.size() < 3) {
                    return Result.Fail();
                }
                // 分块发送
                Result res = sendChunk2DataNode(chunk, nodes);
                if (res != null) {
                    return Result.Fail();
                }
            }
            // 文件状态变更为上传完成
            fm.setFileStatus(FileStatus.Done.getStatus());
            etcdService.put(getFileMetaKey(fileKey), JSON.toJSONString(fm));
            return Result.Success();
        } catch (IOException | ExecutionException | InterruptedException e) {
            e.printStackTrace();
            return Result.Fail();
        }
    }

    private int calcDoneNodes(List<INodeSnapShot> inodes) {
        int counter = 0;
        for (INodeSnapShot inode : inodes) {
            if (inode.getStatus() == FileStatus.Done.getStatus()) {
                counter++;
            }
        }
        return counter;
    }

    private Result preCheckFileStatus(String fileKey) {
        FileMeta fileMeta = getFileMeta(fileKey);
        if (fileMeta != null && FileStatus.Done.getStatus() == fileMeta.getFileStatus()) {
            return Result.Success();
        }
        // todo 后续支持续传
        else if (fileMeta != null && FileStatus.Initial.getStatus() == fileMeta.getFileStatus()) {
            return Result.Fail();
        }
        return null;
    }

    private FileMeta getFileMeta(String fileKey) {
        try {
            String value = etcdService.syncGetValue(fileKey);
            if (value.isEmpty()) {
                return null;
            }
            return (FileMeta) JSON.parse(value);
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
            return null;
        }
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

    private FileMeta buildFileMeta(String fileKey, List<Chunk> chunks, MultipartFile uploadFile) {
        List<String> chunkKeys = new ArrayList<>(chunks.size());
        for (Chunk chunk : chunks) {
            chunkKeys.add(chunk.getChunkKey());
        }
        return new FileMeta(uploadFile.getOriginalFilename(),
                fileKey, uploadFile.getSize(), uploadFile.getContentType(), chunkKeys);
    }

    private String getFileMetaKey(String fileKey) {
        return "FILE_META" + fileKey;
    }

    private Result sendChunk2DataNode(Chunk c, List<DataNode> inodes) throws ExecutionException, InterruptedException {
        try {
            // 预占checkpoint
            List<INodeSnapShot> snapShots = preOccupyDataCheckPoint(inodes);
            // 实际发送
            ChunkMeta res = getSendChunkResult(c, snapShots);
            if (calcDoneNodes(res.getInodes()) < replicaFactor) {
                return Result.Fail();
            }
            return null;
        } catch (BizException | IOException e) {
            e.printStackTrace();
            return Result.Fail();
        }
    }

    private ChunkMeta getSendChunkResult(Chunk c, List<INodeSnapShot> snapShots) throws IOException {
        ChunkMeta cm = new ChunkMeta(c.getChunkKey(), snapShots);
        etcdService.put(getChunkMetaKey(c.getChunkKey()), JSON.toJSONString(cm));
        // 获取DataNode元信息
        Map<String, DataNode> nodes = getNodes(snapShots);
        // 实际发送
        for (int i = 0; i < snapShots.size(); i++) {
            DataNode node = nodes.get(snapShots.get(i).getNodeKey());
            MultipartEntityBuilder me = MultipartEntityBuilder.create();
            StringBody offsetBody = new StringBody(String.valueOf(snapShots.get(i).getOffset()), ContentType.APPLICATION_JSON);
            me.addBinaryBody("chunk", buildChunkBytes(c))
                    .addPart("offset", offsetBody);
            String res = HttpUtil.postEntity(node.getUrl(), me.build());
            if (!res.isEmpty()) {
                cm.getInodes().get(i).setStatus(1);
            }
        }
        // update chunk meta
        etcdService.put(getChunkMetaKey(c.getChunkKey()), JSON.toJSONString(cm));
        return cm;
    }

    private byte[] buildChunkBytes(Chunk c) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(c.getChunkKey().getBytes());
        out.write(NumUtil.intToBinary32(c.getSize(), 32).getBytes());
        out.write(c.getBuffer());
        String chunkMd5 = FileUtil.getMD5sum(out.toByteArray());
        out.write(chunkMd5.getBytes());
        return out.toByteArray();
    }

    private List<INodeSnapShot> preOccupyDataCheckPoint(List<DataNode> inodes) throws BizException, ExecutionException, InterruptedException {
        List<INodeSnapShot> snapShots = new ArrayList<>(inodes.size());
        // 预占DataNode空间
        for (DataNode inode : inodes) {
            int i = 0;
            while (true) {
                if (i == retryTimes) {
                    logger.warn("SendChunk2DataNode#ETCD CAS RETRY REACH LIMIT");
                    throw new BizException("PreOccupyDataCheckPoint Failed");
                }
                DataNode node = getNodeInfo(inode.getKey());
                if (node == null) {
                    logger.warn("SendChunk2DataNode#Node Not Exist");
                    throw new BizException("Node Not Exist");
                }
                DataNode copy = new DataNode();
                BeanUtils.copyProperties(node, copy);
                copy.setCheckpoint(copy.getCheckpoint() + ChunkCapacity);
                if (etcdService.syncCas(getNodeInfoKey(inode.getKey()),
                        JSON.toJSONString(node), JSON.toJSONString(copy))) {
                    snapShots.add(new INodeSnapShot(inode.getKey(), node.getCheckpoint(), 0));
                    break;
                }
                i++;
            }
        }
        return snapShots;
    }

    private Map<String, DataNode> getNodes(List<INodeSnapShot> snapShots) {
        Map<String, DataNode> map = new HashMap<>();
        for (INodeSnapShot snapShot : snapShots) {
            try {
                map.put(snapShot.getNodeKey(), getNodeInfo(snapShot.getNodeKey()));
            } catch (ExecutionException | InterruptedException e) {
                e.printStackTrace();
            }
        }
        return map;
    }


    private String buildPostChunkUrl(String url) {
        return url + "/chunk/put";
    }

    private DataNode getNodeInfo(String key) throws ExecutionException, InterruptedException {
        String value = etcdService.syncGetValue(getNodeInfoKey(key));
        return value.isEmpty() ? null : (DataNode) JSON.parse(value);
    }

    private String getNodeInfoKey(String nodeName) {
        return "DATANODE_KEY_" + nodeName;
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
        for (int seq = 0; seq < bytesList.size(); seq++) {
            byte[] b = bytesList.get(seq);
            String md5Code = FileUtil.getMD5sum(b);
            String chunkKey = fileKey + String.format("%04d", seq);
            Chunk c = new Chunk(chunkKey, b, md5Code, b.length);
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
