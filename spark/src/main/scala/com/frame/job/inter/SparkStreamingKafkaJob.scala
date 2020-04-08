package com.frame.job.inter

import com.frame.constant.AnalystSparkConfConstant
import com.frame.constant.AnalystSparkConfConstant._
import com.frame.core.exception.JobException
import com.frame.tools.conf.Conf
import com.frame.utils.kafka.{ConsumerOffsetHandlerException, OffsetHandler, SparkDirectKafkaManage}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.HasOffsetRanges
/**
  * spark streaming kafka job 抽象实现.
  * 依赖参数参考引入 [[AnalystSparkConfConstant]]相关常量值
  * 如需重写 kafkaParam 请重写 kafkaParamsMap
  * @author XiaShuai on 2020/4/8.
  */
trait SparkStreamingKafkaJob[K, V] extends SparkStreamingJob {
  /** Direct 方式交互 kafka 并管理消费偏移量 */
  private var _kafkaManage: SparkDirectKafkaManage = _

  /** 主要逻辑为初始化 sparkDirectKafka ，创建 InputDStream */
  @throws[JobException]
  override final def streamingJobCompute(): Unit = {
    // 偏移量管理具体实现类配置
    val handlerClassName: String = conf.getString(
      SK_SPARK_STREAMING_KAFKA_OFFSET_HANDLER_CLASS_NAME,
      SK_SPARK_STREAMING_KAFKA_OFFSET_HANDLER_CLASS_NAME_DEFAULT_VAL
    )
    var handler: OffsetHandler = null
    try handler = Class.forName(handlerClassName).newInstance().asInstanceOf[OffsetHandler] catch {
      case e@(_: InstantiationException | _: IllegalAccessException | _: ClassNotFoundException) =>
        throw new JobException(s"newInstance class $handlerClassName error", e)
    }

    val groupId = conf.getString(SK_SPARK_STREAMING_KAFKA_GROUP_ID)
    if (!(groupId == "test_skuld" || groupId == "pro_skuld")) { // 强制校验 groupId 防止修改后消费数据混乱
      throw new IllegalArgumentException("groupId must be in ('test_skuld','pro_skuld')")
    }
    _kafkaManage = new SparkDirectKafkaManage(
      groupId,
      conf.getString(SK_SPARK_STREAMING_KAFKA_CONSUMPTION_STRATEGY),
      handler)

    try streamCompute(_kafkaManage.createDirectStream(streamingContext, topics(), kafkaParamsMap(conf)))
    catch {
      case e@(_: IllegalAccessException | _: ConsumerOffsetHandlerException | _: JobException) =>
        throw new JobException(e)
    }
  }

  /** Kafka params map. 需要注意的是: 如果 Map 中存在相同的 key，合并后的 Map 中的 value 会被最右边的 Map 的值所代替 */
  def kafkaParamsMap(conf: Conf): Map[String, AnyRef] = {
    Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> conf.getString(SK_SPARK_STREAMING_READ_KAFKA_BOOTSTRAP_SERVERS),
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG -> conf.getString(SK_SPARK_STREAMING_KAFKA_GROUP_ID),
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"
    )
  }

  /** 消费 kafka topic */
  def topics(): Set[String]

  /** 扩展 sparkConf SparkStreaming kafka 相关配置 */
  override def buildSparkConf(): SparkConf = {
    super.buildSparkConf()
      .set("spark.streaming.kafka.maxRatePerPartition", conf.getString(SK_SPARK_STREAMING_KAFKA_MAX_RATE_PER_PARTITION))
      .set("spark.streaming.kafka.consumer.poll.ms", "120000")
      .set("spark.streaming.backpressure.enabled", "true") // 开启kafka背压机制
  }

  /** stream 计算抽象方法 */
  @throws[JobException]
  def streamCompute(stream: InputDStream[ConsumerRecord[K, V]]): Unit

  /** 修改偏移量. */
  @throws[JobException]
  final def updateOffset(rdd: RDD[ConsumerRecord[K, V]]): Unit = {
    try {
      this._kafkaManage.updateOffset(rdd.asInstanceOf[HasOffsetRanges])
    } catch {
      case e: ConsumerOffsetHandlerException =>
        throw new JobException(s"updateOffset error , Caused by ${e.getMessage}", e)
    }
  }
}
