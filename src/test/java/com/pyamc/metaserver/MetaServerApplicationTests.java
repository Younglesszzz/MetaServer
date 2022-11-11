package com.pyamc.metaserver;

import com.alibaba.fastjson.JSON;
import com.pyamc.metaserver.entity.Result;
import com.pyamc.metaserver.service.EtcdService;
import com.pyamc.metaserver.service.FileService;
import io.etcd.jetcd.kv.TxnResponse;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.Resource;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

@SpringBootTest
class MetaServerApplicationTests {

    @Resource
    FileService fileService;

    @Resource
    EtcdService etcdService;

    @Test
    void contextLoads() {
    }


    @Test
    void uploadFile() {
        File file = new File("C:\\Users\\Young\\Pictures\\奶茶弟弟.jpg");
        try {
            String contentType = file.toURL().openConnection().getContentType();
            FileInputStream in = new FileInputStream(file);
            byte[] content = Files.readAllBytes(file.toPath());
            MultipartFile result = new MockMultipartFile(file.getName(), file.getName(), contentType, content);
            Result res = fileService.process(result);
            System.out.println(JSON.toJSONString(res));
        } catch (IOException e) {
            e.printStackTrace();
        }

//        try {
//            FileInputStream in = new FileInputStream(file);
////            String contentType = Files.probeContentType(file.getPath());
////            MultipartFile multi = new MockMultipartFile(file.getName(), );
//
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        }
//        fileService.process(file);

    }


    @Test
    void testMultiGet() {
        List<String> list = new ArrayList<>();
        list.add("CHUNKINFO_2945C3ACDD8ADEF1396C54CF9F34DD230000");
        try {
            TxnResponse res = etcdService.multiGet(list);
            res.getGetResponses().get(0).getKvs();

            System.out.println(JSON.toJSONString(res));
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    void testDownload() {
        fileService.download("FILE_KEY_2945C3ACDD8ADEF1396C54CF9F34DD23", new MockHttpServletResponse());
    }
}
