package com.pyamc.metaserver;

import com.alibaba.fastjson.JSON;
import com.pyamc.metaserver.entity.Result;
import com.pyamc.metaserver.service.EtcdService;
import com.pyamc.metaserver.service.FileService;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.Resource;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;

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


}
