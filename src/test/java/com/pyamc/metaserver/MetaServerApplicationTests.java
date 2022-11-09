package com.pyamc.metaserver;

import com.pyamc.metaserver.service.EtcdService;
import com.pyamc.metaserver.service.FileService;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.Resource;

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
        MultipartFile file = null;
        fileService.process(file);

    }


}
