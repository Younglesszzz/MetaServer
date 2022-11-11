package com.pyamc.metaserver.service;

import com.alibaba.fastjson.JSON;
import com.pyamc.metaserver.entity.*;
import com.pyamc.metaserver.exception.BizException;
import com.pyamc.metaserver.util.FileUtil;

import java.io.ByteArrayOutputStream;
import java.lang.String;

import com.pyamc.metaserver.util.HttpUtil;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.TxnResponse;
import io.etcd.jetcd.options.GetOption;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.Resource;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.ByteBuffer;
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
    private static final int ChunkKeyBytesNum = 36;
    private static final int ChunkSizeBytesNum = 4;
    private static final int FileBytesSize = ChunkCapacity - ChunkKeyBytesNum - ChunkSizeBytesNum - CheckSumBytesNum;
    private static final int RETRY_TIMES = 3;
    private static final int REPLICA_FACTOR = 3;

    public void download(String fileKey, HttpServletResponse response) {
        // 1.获取文件元信息
        FileMeta fm = getFileMeta(fileKey);
        if (fm == null) {
            logger.warn("Download#Error FileKey {} MetaData Not Exist", fileKey);
            return ;
        }
        // 2.合并文件流
        List<String> keys = fm.getChunkKeys();
        byte[] data = mergeFileStream(keys);
        if (data == null || data.length == 0) {
            logger.warn("Merge File Stream Is Empty {}", fileKey);
            return;
        }
        // 3.返回
        try {
            ServletOutputStream out = response.getOutputStream();
            String cd = String.format("attachment; filename=\"%s\"", fm.getFileName());
            String ct = String.format("%s; name=\"%s\"", fm.getContentType(), fm.getFileName());
            response.setContentType(ct);
            response.setHeader("Content-Disposition", cd);
            out.write(data);
            out.flush();
        } catch (IOException e) {
            logger.error("Download IOException {}", e.getMessage());
            e.printStackTrace();
        }

    }

    private byte[] mergeFileStream(List<String> keys) {
        // 获取etcd 信息
        try {
            TxnResponse res = etcdService.multiGet(keys);
            List<GetResponse> gets = res.getGetResponses();
            List<ChunkMeta> metas = new ArrayList<>();
            for (GetResponse get : gets) {
                String val = get.getKvs().get(0).getValue().toString();
                ChunkMeta cm = JSON.parseObject(val, ChunkMeta.class);
                metas.add(cm);
            }
            // in case of disorder
            metas.sort(Comparator.comparing(ChunkMeta::getChunkKey));
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            // 获取单一chunk byte[]
            for (ChunkMeta meta : metas) {
                byte[] b = getHttpRetrieveChunkResult(meta);
                logger.warn("RetrieveChunkResult IS NULL {}", JSON.toJSONString(meta));
                if (b == null || b.length == 0) {
                    return null;
                }
                out.write(b);
            }
            return out.toByteArray();
        } catch (ExecutionException | InterruptedException | IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private byte[] getHttpRetrieveChunkResult(ChunkMeta meta) {
        List<INodeSnapShot> shots = meta.getInodes();
        try {
            // hash算法
            int i = 0;
            int start = meta.getChunkKey().hashCode() % REPLICA_FACTOR;
            while (true) {
                if (i == RETRY_TIMES) {
                    logger.error("RETRY TIMES FAIL");
                    return null;
                }
                INodeSnapShot shot = shots.get(start);
                String val = etcdService.syncGetValue(shot.getNodeKey());
                DataNode node = JSON.parseObject(val, DataNode.class);
                MultipartEntityBuilder entityBuilder = MultipartEntityBuilder.create();
                entityBuilder.addTextBody("offset", String.valueOf(shot.getOffset()));
                entityBuilder.addTextBody("chunkKey", meta.getChunkKey());
                byte[] data = HttpUtil.getPostBytes(buildGetChunkUrl(node.getUrl()), entityBuilder.build());
                if (data != null && data.length > 0) {
                    return data;
                }
                start = start == REPLICA_FACTOR - 1 ? 0 : start + 1;
                i++;
            }
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }

        return null;
    }


    public Result process(MultipartFile uploadFile) {
        logger.info("Process#uploadFile: {}", uploadFile.getName());
        String fileKey = null;
        try {
            byte[] bytes = uploadFile.getBytes();
            String contentType = uploadFile.getContentType();
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
                Result res = sendChunk2DataNode(contentType, chunk, nodes);
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
            if (value == null || value.isEmpty()) {
                return null;
            }
            return JSON.parseObject(value, FileMeta.class);
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
        return "FILE_KEY_" + fileKey;
    }

    private Result sendChunk2DataNode(String contentType, Chunk c, List<DataNode> inodes) throws ExecutionException, InterruptedException {
        try {
            // calc send bytes
            byte[] chunkBytes = buildChunkBytes(c);
            // 预占checkpoint
            List<INodeSnapShot> snapShots = preOccupyDataCheckPoint(chunkBytes.length, inodes);
            // 实际发送
            ChunkMeta res = getSendChunkResult(chunkBytes, c, contentType, snapShots, inodes);
            if (calcDoneNodes(res.getInodes()) < REPLICA_FACTOR) {
                return Result.Fail();
            }
            return null;
        } catch (BizException | IOException e) {
            e.printStackTrace();
            return Result.Fail();
        }
    }

    private ChunkMeta getSendChunkResult(byte[] chunkBytes, Chunk c, String contentType, List<INodeSnapShot> snapShots, List<DataNode> inodes) throws IOException {
        ChunkMeta cm = new ChunkMeta(c.getChunkKey(), snapShots);
        etcdService.put(getChunkMetaKey(c.getChunkKey()), JSON.toJSONString(cm));
        // 获取DataNode元信息
        // 实际发送
        for (int i = 0; i < snapShots.size(); i++) {
            DataNode node = inodes.get(i);
            MultipartEntityBuilder entityBuilder = MultipartEntityBuilder.create();
            entityBuilder.addTextBody("offset", String.valueOf(snapShots.get(i).getOffset()));
            entityBuilder.addBinaryBody("chunk", chunkBytes, ContentType.parse(contentType), c.getChunkKey());
            String res = HttpUtil.getPostStr(buildPostChunkUrl(node.getUrl()), entityBuilder.build());
            if (!res.isEmpty()) {
                cm.getInodes().get(i).setStatus(1);
            }
        }
        // update chunk meta
        etcdService.put(getChunkMetaKey(c.getChunkKey()), JSON.toJSONString(cm));
        return cm;
    }

    // 构造实际存储块
    private byte[] buildChunkBytes(Chunk c) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(c.getChunkKey().getBytes());
        out.write(ByteBuffer.allocate(4).putInt(c.getSize()).array());
        out.write(c.getBuffer());
        String chunkMd5 = FileUtil.getMD5sum(out.toByteArray());
        out.write(chunkMd5.getBytes());
        return out.toByteArray();
    }

    private List<INodeSnapShot> preOccupyDataCheckPoint(int length, List<DataNode> inodes) throws BizException, ExecutionException, InterruptedException {
        List<INodeSnapShot> snapShots = new ArrayList<>(inodes.size());
        // 预占DataNode空间
        for (int i = 0; i < inodes.size(); i++) {
            DataNode inode = inodes.get(i);
            int retryCounter = 0;
            while (true) {
                if (retryCounter == RETRY_TIMES) {
                    logger.warn("SendChunk2DataNode#ETCD CAS RETRY REACH LIMIT");
                    throw new BizException("PreOccupyDataCheckPoint Failed");
                }
                // 1.尝试cas checkpoint
                String origin = JSON.toJSONString(inode);
                long checkpoint = inode.getCheckpoint();
                inode.setCheckpoint(checkpoint + length);
                String update = JSON.toJSONString(inode);
                if (etcdService.syncCas(inode.getKey(), origin, update)) {
                    snapShots.add(new INodeSnapShot(inode.getKey(), checkpoint, 0));
                    break;
                }
                // 2.重新获取dataNode信息
                DataNode newInode = getNodeInfo(inode.getKey());
                if (newInode == null) {
                    logger.warn("SendChunk2DataNode#Get Inode Fail {}", inode.getKey());
                    throw new BizException("Node Not Exist");
                }
                inode = newInode;
                retryCounter++;
            }
        }
        return snapShots;
    }

    private String buildPostChunkUrl(String url) {
        return url + "/chunk/put";
    }

    private String buildGetChunkUrl(String url) {
        return url + "/chunk/get";
    }

    private DataNode getNodeInfo(String inodeKey) throws ExecutionException, InterruptedException {
        String value = etcdService.syncGetValue(inodeKey);
        if (value == null || value.isEmpty()) {
            return null;
        }
        return JSON.parseObject(value, DataNode.class);
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
            DataNode t = JSON.parseObject(value, DataNode.class);
            res.add(t);
        }
        return res;
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

}
