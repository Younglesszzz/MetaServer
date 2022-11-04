package com.pyamc.metaserver.service;

import com.pyamc.metaserver.entity.DataNode;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.watch.WatchEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

@Service
public class DataService {
    private static Map<String, DataNode> dataCluster = new ConcurrentHashMap<String, DataNode>();
    private final Map<String, Watch.Watcher> watcherMap = new ConcurrentHashMap<String, Watch.Watcher>();
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private static final String watchKey = "DATANODE_CLUSTER";

    @Resource
    EtcdService etcdService;

    @PostConstruct
    public void init() {
        Watch.Listener listener = Watch.listener(watchResponse -> {
            logger.info(" Key: {} Receive Events: {}", watchKey,
                    Arrays.toString(watchResponse.getEvents().toArray()));
            watchResponse.getEvents().forEach(watchEvent -> {
                // 操作类型
                WatchEvent.EventType eventType = watchEvent.getEventType();

                // 操作的键值对
                KeyValue keyValue = watchEvent.getKeyValue();

                logger.info("type={}, key={}, value={}",
                        eventType,
                        keyValue.getKey().toString(),
                        keyValue.getValue().toString()
                );
                // 如果是更新操作，重新编译数据节点
                if (WatchEvent.EventType.PUT.equals(eventType)) {
                    parseDataNodeCluster(watchKey);
                }

                // 如果是删除操作，就把该key的Watcher找出来close掉
                if (WatchEvent.EventType.DELETE.equals(eventType)
                        && watcherMap.containsKey(watchKey)) {
                    Watch.Watcher watcher = watcherMap.remove(watchKey);
                    watcher.close();
                    dataCluster.clear();
                }
            });
        });

        Watch.Watcher watcher = null;
        try {
            watcher = etcdService.watch(watchKey, listener);
        } catch (Exception e) {
            e.printStackTrace();
        }
        watcherMap.put(watchKey, watcher);
    }

    // 编译数据节点集群
    public void parseDataNodeCluster(String key) {
        String watchValue = null;
        try {
            watchValue = etcdService.syncGetValue(watchKey);
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
        if (watchValue == null) {
            logger.warn("parseDataNodeCluster#Get Watch Value Failed");
            return;
        }
        String[] data = watchValue.split(";");
        ConcurrentHashMap<String, DataNode> copy = new ConcurrentHashMap<>();
        for (String datum : data) {
            String[] duo = datum.split(":");
            if (dataCluster.containsKey(duo[0])) {
                copy.put(duo[0], dataCluster.get(duo[0]));
            } else {
                copy.put(duo[0], new DataNode(duo[0], duo[1]));
            }
        }
        dataCluster = copy;
    }
}
