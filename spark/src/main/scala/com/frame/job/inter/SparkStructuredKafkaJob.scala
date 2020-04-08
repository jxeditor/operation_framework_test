package com.frame.job.inter

import com.frame.core.exception.JobException
import com.frame.job.SparkJob
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.streaming.{DataStreamWriter, ProcessingTime, StreamingQuery, Trigger}
import com.frame.constant.AnalystSparkConfConstant._

/**
  * spark structured streaming kafka job 抽象实现.
  * 设计为 in（kafka） -> process（抽象实现） -> out（抽象实现）
  * @author XiaShuai on 2020/4/8.
  */
trait SparkStructuredKafkaJob[Out] extends SparkJob {
  /** 返回数据字段 "topic", "partition", "offset", "key", "value" */
  type KafkaRecord = Dataset[(String, Int, Long, Array[Byte], Array[Byte])]
  protected lazy val trigger: Trigger = ProcessingTime(s"${conf.getInt(SK_SPARK_STREAMING_DURATION_SECONDS)} seconds")

  @throws[JobException]
  override final def jobCompute(): Unit = {
    // in -> process -> out
    val outStream: Dataset[Out] = process(in = in())
    logWarn(s"out DataFrame.schema = ${outStream.schema.treeString}")
    val q: StreamingQuery = out(process = outStream).queryName(this.getClass.getSimpleName).start()
    // val q: StreamingQuery = out(process = outStream).start()
    q.awaitTermination()
    sys.addShutdownHook(q.stop())
  }

  def in(): KafkaRecord = {
    val ss: SparkSession = sparkSession
    import ss.implicits._
    sparkSession // 读取kafka数据
      .readStream
      .format("kafka")
      // kafka 相关配置 option,详细参考官网  http://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html#
      // creating-a-kafka-source-for-batch-queries
      .option("kafka.bootstrap.servers", conf.getString(SK_SPARK_STREAMING_READ_KAFKA_BOOTSTRAP_SERVERS))
      .option(inTopic()._1, inTopic()._2)
      .option("kafkaConsumer.pollTimeoutMs", "512") // 轮询执行数据，以毫秒为超时间隔单位
      .option("fetchOffset.numRetries", "5") // 重试次数
      .option("fetchOffset.retryIntervalMs", "500") // 重试等待时间 毫秒
      // 每批次最大偏移量,每个topic读取的偏移量等于设定值除以topic总数
      .option("maxOffsetsPerTrigger", conf.getString(SK_SPARK_STRUCT_KAFKA_MAX_OFFSETS_PER_TRIGGER))
      // .option("startingOffsets", "200") // startingOffsets
      // .option("endingOffsets", "200") // endingOffsets
      .option("failOnDataLoss", "true") // 当数据可能丢失时是否使查询失败
      .load()
      .selectExpr("topic", "partition", "offset", "key", "value")
      .as[(String, Int, Long, Array[Byte], Array[Byte])]
  }

  /**
    * 读取 kafka topic option
    * 仅支持 "assign", "subscribe" or "subscribePattern" 其中一种
    * subscribe        | topics 多个以 `,` 隔开
    * assign           | json string {"topicA":[0,1],"topicB":[2,4]}
    * subscribePattern | Java regex string
    */
  def inTopic(): (String, String)

  /** stream 计算抽象方法 */
  @throws[JobException]
  def process(in: KafkaRecord): Dataset[Out]

  def out(process: Dataset[Out]): DataStreamWriter[Out]

}
