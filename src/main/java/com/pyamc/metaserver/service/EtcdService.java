package com.pyamc.metaserver.service;

import io.etcd.jetcd.*;
import io.etcd.jetcd.kv.DeleteResponse;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.PutResponse;
import io.etcd.jetcd.kv.TxnResponse;
import io.etcd.jetcd.op.Cmp;
import io.etcd.jetcd.op.CmpTarget;
import io.etcd.jetcd.op.Op;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;
import io.etcd.jetcd.watch.WatchEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.xml.crypto.Data;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

@Service
public class EtcdService {
    private final Client client = Client.builder().endpoints("http://127.0.0.1:2379", "http://127.0.0.1:22379", "http://127.0.0.1:32379").build();
    private final KV kvClient = client.getKVClient();
    private final Watch watchClient = client.getWatchClient();
    private static final String watchKey = "DATANODE_CLUSTER";
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    // set a key if not exists
    public TxnResponse setNx(String k, String v) throws ExecutionException, InterruptedException {
        ByteSequence bsKey = byteSequenceOf(k);
        ByteSequence bsValue = byteSequenceOf(v);

        Cmp cmp = new Cmp(bsKey, Cmp.Op.EQUAL, CmpTarget.version(0));
        TxnResponse txnResponse = kvClient.txn().If(cmp).
                Then(Op.put(bsKey, bsValue, PutOption.DEFAULT)).commit().get();
        return txnResponse;
    }

    public CompletableFuture<PutResponse> put(String k, String v) {
        ByteSequence key = ByteSequence.from(k.getBytes());
        ByteSequence value = ByteSequence.from(v.getBytes());
        return kvClient.put(key, value);
    }


    public CompletableFuture<DeleteResponse> delete(String k) {
        ByteSequence key = ByteSequence.from(k.getBytes());
        return kvClient.delete(key);
    }

    public CompletableFuture<GetResponse> get(String k) {
        ByteSequence key = ByteSequence.from(k.getBytes());
        return kvClient.get(key);
    }

    public String syncGetValue(String k) throws ExecutionException, InterruptedException {
        List<KeyValue> kvs = get(k).get().getKvs();
        if (kvs == null || kvs.size() == 0) {
            return null;
        }
        return kvs.get(0).getValue().toString();
    }

    public CompletableFuture<GetResponse> getWithOption(String prefix, GetOption option) {
        return kvClient.get(byteSequenceOf(prefix), option);
    }

    public boolean syncCas(String k, String expectValue, String updateValue) throws ExecutionException, InterruptedException {
        ByteSequence bsKey = byteSequenceOf(k);
        ByteSequence bsExpect = byteSequenceOf(expectValue);
        ByteSequence bsUpdate = byteSequenceOf(updateValue);

        Cmp cmp = new Cmp(bsKey, Cmp.Op.EQUAL, CmpTarget.value(bsExpect));

        TxnResponse txnResponse = kvClient.txn().If(cmp)
                .Then(Op.put(bsKey, bsUpdate, PutOption.DEFAULT)).commit().get();
        return txnResponse.isSucceeded() && !txnResponse.getPutResponses().isEmpty();
    }

    public Watch.Watcher watch(String key, Watch.Listener listener) throws Exception {
        return watchClient.watch(byteSequenceOf(key), listener);
    }

    private ByteSequence byteSequenceOf(String s) {
        return ByteSequence.from(s.getBytes());
    }
}
