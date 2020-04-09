package com.frame.tools.string;

import com.frame.tools.lang.Nullable;
import com.google.common.base.Strings;

/**
 * @author XiaShuai on 2020/4/8.
 */
public class StringUtil {
    private StringUtil() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * 判断是否为 null or empty
     *
     * @param str str
     * @return boolean boolean
     */
    public static boolean isNullOrEmpty(String str) {
        return str == null || str.length() == 0;
    }

    /**
     * 判断是否为 null or 去空格后是否为 empty
     *
     * @param str str
     * @return boolean boolean
     */
    public static boolean isNullOrTrimEmpty(String str) {
        return str == null || str.trim().length() == 0;
    }


    public static boolean isNotNullOrEmpty(@Nullable String s) {
        return !(s == null || "".equals(s));
    }

    public static boolean isNullOrEmpty(@Nullable String... s) {
        if (s == null || s.length == 0) {
            return true;
        }
        boolean b = false;
        for (String s1 : s) {
            if (isNullOrEmpty(s1)) {
                b = true;
                break;
            }
        }
        return b;
    }

    /**
     * 在原始字符串两端添加指定字符
     * <p>
     * e.g
     * System.out.println(headTailPadding("original str", "~", 100));
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~original str~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     *
     * @param original original
     * @param padding  padding
     * @param length   返回总体长度
     * @return str
     */
    public static String headTailPadding(String original, String padding, int length) {
        if (original == null || original.length() >= length) {
            return original;
        }
        final int paddingLength = (length - original.length()) / padding.length() / 2;
        final String repeat = Strings.repeat(padding, paddingLength);
        return repeat + original + repeat;
    }
}
