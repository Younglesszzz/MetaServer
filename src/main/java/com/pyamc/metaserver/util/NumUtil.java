package com.pyamc.metaserver.util;

public class NumUtil {
    //i 期望转换的整数 bitNum 期望转换的二进制字符串位数
    public static String intToBinary32(int i, int bitNum){
        String binaryStr = Integer.toBinaryString(i);
        while(binaryStr.length() < bitNum){
            binaryStr = "0" + binaryStr;
        }
        return binaryStr;
    }
}
