package com.pyamc.metaserver.controller;

import com.pyamc.metaserver.entity.Result;
import com.pyamc.metaserver.service.FileService;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import reactor.netty.http.client.HttpClient;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletResponse;

@RestController
@RequestMapping("/file")
public class FileController {
    @Resource
    FileService fileService;

    @RequestMapping("/get")
    public void get(@RequestParam String fileKey, HttpServletResponse response) {
        fileService.download(fileKey, response);
    }

    @PostMapping(value = "/put")
    @ResponseBody
    public Result put(@RequestParam("uploadFile") MultipartFile uploadFile,
                         @RequestParam("fileTagName") String fileTagName)  {
        fileService.process(uploadFile);
        return Result.Success();
    }
}
