package com.frame.constant;

import com.frame.tools.conf.Conf;
import com.frame.tools.conf.ConfBuilder;
import com.frame.tools.conf.ConfConstant;
import com.frame.utils.kafka.MysqlOffsetHandler;

/**
 * @author XiaShuai on 2020/4/8.
 */
public class AnalystSparkConfConstant implements ConfConstant {
    /**
     * 运行任务类名
     */
    public static final String SK_SPARK_CLASS_NAME =
            "sk.spark.className";

    /**
     * spark 任务 appName
     */
    public static final String SK_SPARK_APP_NAME =
            "sk.spark.appName";

    /**
     * mysql.kafka_consumer JdbcUrl
     */
    public static final String SK_MYSQL_KAFKA_CONSUMER_JDBC_URL =
            "sk.mysql.kafkaConsumer.JdbcUrl";

    /**
     * * 最早记录消费 BEGINNING_OFFSET
     * * 最晚记录消费 END_OFFSET
     *
     * @see com.frame.utils.kafka.ConsumptionStrategy
     */
    public static final String SK_SPARK_STREAMING_KAFKA_CONSUMPTION_STRATEGY =
            "sk.spark.streaming.kafka.consumptionStrategy";

    /**
     * spark.streaming.读取 kafka
     */
    public static final String SK_SPARK_STREAMING_READ_KAFKA_BOOTSTRAP_SERVERS =
            "sk.spark.streaming.read_kafka.bootstrap.servers";

    /**
     * spark.streaming.kafka 实时任务每秒每分区获取最大数据量
     */
    public static final String SK_SPARK_STREAMING_KAFKA_MAX_RATE_PER_PARTITION =
            "sk.spark.streaming.kafka.maxRatePerPartition";
    /**
     * spark.streaming.kafka 实时任务每秒每分区获取最大数据量
     */
    public static final String SK_SPARK_STRUCT_KAFKA_MAX_OFFSETS_PER_TRIGGER =
            "sk.spark.struct.kafka.maxOffsetsPerTrigger";

    /**
     * spark 实时任务运行时间间隔
     * 用于 spark streaming & spark structured streaming（trigger option）
     */
    public static final String SK_SPARK_STREAMING_DURATION_SECONDS =
            "sk.spark.streaming.durationSeconds";

    public static final String SK_SPARK_SQL_PRINT_EXPLAIN = "sk.spark.sql.print.explain";

    /**
     * The Sk spark streaming kafka consumer offset handler class name.
     */
    public static final String SK_SPARK_STREAMING_KAFKA_OFFSET_HANDLER_CLASS_NAME =
            "sk.spark.streaming.kafka.offsetHandlerClassName";
    /**
     * The Sk spark streaming kafka consumer offset handler class name default val.
     */
    public static final String SK_SPARK_STREAMING_KAFKA_OFFSET_HANDLER_CLASS_NAME_DEFAULT_VAL =
            MysqlOffsetHandler.class.getName();
    /**
     * The Sk spark streaming kafka group id.
     */
    public static final String SK_SPARK_STREAMING_KAFKA_GROUP_ID =
            "sk.spark.streaming.kafka.group_id";

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

    private AnalystSparkConfConstant() {
    }

    public static Conf getConf() {
        return CONF;
    }
}
