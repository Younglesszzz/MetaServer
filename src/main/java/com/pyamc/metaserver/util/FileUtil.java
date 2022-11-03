package com.pyamc.metaserver.util;

import org.springframework.stereotype.Component;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class FileUtil {
    // 获取文件md5
    public static String getMD5sum(byte[] bytes) {
        try {
            if (bytes.length > 0) {
                MessageDigest md5 = MessageDigest.getInstance("md5");
                md5.update(bytes, 0, bytes.length);
                byte[] code = md5.digest();
                return byte2hex(code);
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return "";
    }

    // 二行制转字符串
    protected static String byte2hex(byte[] b) {
        String hs = "";
        String stmp = "";
        for (int n = 0; n < b.length; n++) {
            stmp = (java.lang.Integer.toHexString(b[n] & 0XFF));
            if (stmp.length() == 1) {
                hs = hs + "0" + stmp;
            } else {
                hs = hs + stmp;
            }
        }
        return hs.toUpperCase();
    }
}
