package com.frame.tools.convert;

/**
 * 对象转 JSON 字符串
 *
 * @author XiaShuai on 2020/4/8.
 */
public interface JsonConvert {
    /**
     * To json byte byte [ ].
     *
     * @return the byte [ ]
     */
    default byte[] toJSONByte() {
        return this.toJSONByte(this);
    }

    /**
     * To json string string.
     *
     * @return the string
     */
    default String toJSONString() {
        return this.toJSONString(this);
    }

    /**
     * To json prettily string string.
     *
     * @return the string
     */
    default String toJSONPrettilyString() {
        return this.toJSONPrettilyString(this);
    }

    /**
     * To json byte byte [ ].
     *
     * @param o the o
     * @return the byte [ ]
     */
    byte[] toJSONByte(Object o);

    /**
     * 对象转换为 string 实现
     *
     * @param o o
     * @return String string
     */
    String toJSONString(Object o);

    /**
     * 对象转换为 美化格式string 实现
     *
     * @param o o
     * @return String string
     */
    String toJSONPrettilyString(Object o);
}
