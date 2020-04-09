package com.frame.constant;

import com.frame.tools.conf.Conf;
import com.frame.tools.conf.ConfBuilder;
import com.frame.tools.conf.ConfConstant;

/**
 * @author XiaShuai on 2020/4/8.
 */
public class AnalystFlinkConfConstant implements ConfConstant {

    /**
     * mysql.kafka_consumer JdbcUrl
     */
    public static final String SK_MYSQL_KAFKA_CONSUMER_JDBC_URL =
            "sk.mysql.kafkaConsumer.JdbcUrl";

    /**
     * 配置参数固定前缀
     */
    public static final String CONF_PREFIX = "sk";

    /**
     * 载入 resource/*.properties 配置文件
     * 所有配置项以 sk 开头
     *
     * @see #getConf()
     */
    private static final Conf CONF = ConfBuilder.create(CONF_PREFIX)
            .withFallbackResource("*.properties")
            .build();

    private AnalystFlinkConfConstant() {
    }

    public static Conf getConf() {
        return CONF;
    }
}
