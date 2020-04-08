package com.frame.tools.io;

import java.io.File;
import java.nio.charset.Charset;

/**
 * IO 相关操作工具类
 * @author XiaShuai on 2020/4/8.
 */
public class IOConstant {

    private IOConstant() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * Charset utf-8
     */
    public static final Charset CHARSET_UTF_8 = Charset.forName("utf-8");
    /**
     * 路径连接符
     */
    public static final String SEPARATOR = File.separator;
    /**
     * 换行符
     */
    public static final String LINE_SEPARATOR = System.getProperty("line.separator");
    /**
     * 换行符 byte[] UTF-8格式
     */
    public static final byte[] LINE_SEPARATOR_BYTE_UTF = LINE_SEPARATOR.getBytes(CHARSET_UTF_8);
}
