package com.frame.tools.convert;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializeWriter;
import com.alibaba.fastjson.serializer.SerializerFeature;

import java.nio.charset.Charset;

import static com.alibaba.fastjson.serializer.SerializerFeature.*;
import static com.frame.tools.convert.SerializerFeatureSetting.*;

/**
 * @author XiaShuai on 2020/4/8.
 */
public interface FastJsonConvert extends JsonConvert {
    /**
     * The constant JSON_CONVERT.
     */
    FastJsonConvert JSON_CONVERT = new FastJsonConvert() {
    };

    /**
     * toJSONByte
     * 使用UTF-8时直接通过 {@link SerializeWriter#toBytes(Charset)} 内置优化写出为 byte[]
     *
     * @param o the o
     * @return byte[]
     */
    @Override
    default byte[] toJSONByte(Object o) {
        return JSON.toJSONBytes(o, SERIALIZER_FEATURES);
    }

    /**
     * 对象转换为 string 实现
     *
     * @param o o
     * @return String
     */
    @Override
    default String toJSONString(Object o) {
        return JSON.toJSONString(o, SERIALIZER_FEATURES);
    }

    /**
     * 对象转换为 美化格式string 实现
     *
     * @param o o
     * @return String
     */
    @Override
    default String toJSONPrettilyString(Object o) {
        return JSON.toJSONString(o, SERIALIZER_PRETTILY_FEATURES);
    }


}

/**
 * 私有配置常量
 */
class SerializerFeatureSetting {

    /**
     * The Serializer features.
     */
    static final SerializerFeature[] SERIALIZER_FEATURES = {DisableCircularReferenceDetect};

    /**
     * The Serializer prettily features.
     */
    static final SerializerFeature[] SERIALIZER_PRETTILY_FEATURES = {DisableCircularReferenceDetect, PrettyFormat};

    private SerializerFeatureSetting() {

    }
}

