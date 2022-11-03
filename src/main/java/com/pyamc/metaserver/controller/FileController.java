package com.pyamc.metaserver.controller;

import com.pyamc.metaserver.entity.Result;
import com.pyamc.metaserver.service.FileService;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.Resource;

@RestController
@RequestMapping("/file")
public class FileController {
    @Resource
    FileService fileService;

    @RequestMapping("/get")
    public Result get(@RequestParam String fileKey) {

        return new Result();
    }

    @PostMapping(value = "/put")
    @ResponseBody
    public Result put(@RequestParam("uploadFile") MultipartFile uploadFile,
                         @RequestParam("fileTagName") String fileTagName)  {

        // 切分
        fileService.process(uploadFile);

        return new Result();
    }
}
