package com.frame.utils.kafka

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.frame.log.Logging
import com.frame.tools.convert.FastJsonConvert
import com.frame.tools.io.IOConstant
import com.frame.tools.lang.Nullable
import com.frame.utils.kafka.SparkDirectKafkaManage.log
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.json4s.jackson.Serialization
import org.json4s.{Formats, NoTypeHints}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
  * kafka 偏移量管理及 DirectStream 方式创建流
  * @author XiaShuai on 2020/4/8.
  */
class SparkDirectKafkaManage(groupId: String,
                              consumptionStrategy: String,
                              offsetHandler: OffsetHandler) extends FastJsonConvert with Serializable {
  implicit val topicPartitionLongOrdering: Ordering[(TopicPartition, Long)] = new Ordering[(TopicPartition, Long)] {
    override def compare(x: (TopicPartition, Long), y: (TopicPartition, Long)): Int = {
      topicPartitionOrdering.compare(x._1, y._1)
    }
  }
  implicit val topicPartitionOrdering: Ordering[TopicPartition] = new Ordering[TopicPartition] {
    override def compare(x: TopicPartition, y: TopicPartition): Int = {
      if (x.topic == y.topic) {
        x.partition - y.partition
      } else {
        x.topic.compareTo(y.topic)
      }
    }
  }

  @throws[IllegalAccessException]
  @throws[ConsumerOffsetHandlerException]
  def createDirectStream[K, V](context: StreamingContext, topicsSet: Set[String], kafkaParams: Map[String, AnyRef]):
  InputDStream[ConsumerRecord[K, V]] = {
    KafkaUtils.createDirectStream(context, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe(topicsSet, kafkaParams, computeOffSets(topicsSet, kafkaParams)))
  }

  private def computeOffSets[K, V](topicsSet: Set[String], kafkaParams: Map[String, AnyRef]):
  Map[TopicPartition, Long] = {
    log.warn(s"createDirectStream , topicsSet $topicsSet")

    // object->json
    implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)
    import org.json4s.jackson.Serialization.writePretty

    log.warn(s"kafkaParam: ${writePretty(kafkaParams.toList.sortBy(m => m._1))}")

    // 第三方保存 消费偏移量详情
    val consumer = this.offsetHandler.fetchCheckIllegal(groupId, topicsSet)
    if (consumer.isEmpty) {
      throw new IllegalArgumentException("kafka.consumer data is empty ! Please check！！！")
    }
    log.warn(s"historical consumer: " +
      s"${
        consumer.values.toSeq.sorted
          .map(f => s"${f.topicPar.topic}\t${f.topicPar.partition}\t offset:[${f.untilOffset}]")
          .mkString(IOConstant.LINE_SEPARATOR, IOConstant.LINE_SEPARATOR, IOConstant.LINE_SEPARATOR)
      }")
    val realRecord = {
      val consumer = new KafkaConsumer[K, V](kafkaParams.asJava)
      val assign = new util.ArrayList[TopicPartition]()
      consumer.listTopics().asScala.filterKeys(topicsSet.contains).values
        .map(_.iterator().asScala.map(t => new TopicPartition(t.topic(), t.partition())))
        .foreach(_.foreach(assign.add))
      val endOffsets = consumer.endOffsets(assign) // 真实偏移量 最晚记录
      consumer.beginningOffsets(assign).asScala.map({ f => // 真实偏移量 最早记录 进行遍历
        TopicPar(f._1.topic(), f._1.partition()) ->
          TopicParOffsetBean(TopicPar(f._1.topic(), f._1.partition()), f._2, endOffsets.get(f._1))
      }).toMap
    }
    // kafka 目前主题-分区 真实的偏移量详情
    log.warn("realRecord: " +
      s"${
        realRecord.values.toSeq.sorted
          .map(f => s"${f.topicPar.topic}\t${f.topicPar.partition}\t offset:[${f.begOffset}~${f.endOffset}]")
          .mkString(IOConstant.LINE_SEPARATOR, IOConstant.LINE_SEPARATOR, IOConstant.LINE_SEPARATOR)
      }")

    // 最终交于 DirectStream 消费偏移量详情
    val subscribeOffsets: Map[TopicPartition, Long] = realRecord.map(m => {
      val tp = new TopicPartition(m._1.topic, m._1.partition) // 实际存在 TopicPartition
      val real = m._2 // 实际偏移量
      val beginningOffset = real.begOffset
      val endOffset = real.endOffset

      val record = consumer.get(m._1) // 第三方记录 TopicPartition
      if (record.isEmpty) { // 历史不存在消费记录
        val fetchStrategyOffset = ConsumptionStrategy.fetchOffset(this.consumptionStrategy, real)
        log.warn(s"[$tp] historical does not exist , use strategy [$consumptionStrategy], " +
          s"realOffset[$beginningOffset->${real.endOffset}] , offset [$fetchStrategyOffset]")
        (tp, fetchStrategyOffset)
      } else { // 历史存在消费，判断历史偏移量是否小于 kafka 目前实际偏移量（例如 kafka 因删除策略执行历史数据删除）
        val consumerOffset = record.get.untilOffset
        if (consumerOffset < beginningOffset) { // 当前历史消费偏移量小于当前 kafka 最小偏移量
          log.warn(s"[$tp] historical exist , consumer < beginning , " +
            s"realOffset[$beginningOffset->$endOffset] , fetchOffset [$beginningOffset]")
          (tp, beginningOffset)
        } else if (consumerOffset > endOffset) { // 当前历史消费偏移量大于当前 kafka 最大偏移量
          log.warn(s"[$tp] historical exist , consumer > end , " +
            s"realOffset[$beginningOffset->$endOffset] , fetchOffset [$endOffset]")
          (tp, endOffset)
        } else { // 当前历史消费偏移量正常情况
          (tp, consumerOffset)
        }
      }
    })
    log.warn(s"subscribeOffsets: " +
      s"${
        subscribeOffsets.toSeq.sorted
          .map(f => s"${f._1.topic()}\t${f._1.partition}\t offset:[${f._2}]")
          .mkString(IOConstant.LINE_SEPARATOR, IOConstant.LINE_SEPARATOR, IOConstant.LINE_SEPARATOR)
      }")

    subscribeOffsets
  }

  /**
    * 更新偏移量.rdd 转换为[[ConsumerOffsetBean]]
    * http://spark.apache.org/docs/2.4.0/streaming-kafka-0-10-integration.html#your-own-data-store
    */
  @throws[ConsumerOffsetHandlerException]
  def updateOffset(hasOffsetRanges: HasOffsetRanges): Unit = {
    val seq: Seq[ConsumerOffsetBean] = hasOffsetRanges.offsetRanges.map((f: OffsetRange) => {
      ConsumerOffsetBean(groupId, TopicPar(f.topic, f.partition), f.fromOffset, f.untilOffset)
    }).toSeq.sorted

    log.warn(s"updateOffset: " +
      s"${
        seq.map(f => s"${f.topicPar.topic}\t${f.topicPar.partition}\t " +
          s"offset:[${f.fromOffset}~${f.untilOffset}] , count[${f.count()}]")
          .mkString(IOConstant.LINE_SEPARATOR, IOConstant.LINE_SEPARATOR, IOConstant.LINE_SEPARATOR)
      }")

    this.offsetHandler.replaceInto(seq)
    // log.warn(s"updateOffset offset result replaceInto ${util.Arrays.toString(replaceInto)}")
  }
}


object SparkDirectKafkaManage {
  private val log = LoggerFactory.getLogger(classOf[SparkDirectKafkaManage])
}


/** 第三方偏移量记录消费操作接口 */
trait OffsetHandler extends Serializable with Logging {

  /** 获取历史数据记录 */
  @Nullable
  @throws[ConsumerOffsetHandlerException]
  def fetch(groupId: String, topicsSet: Set[String]): Seq[ConsumerOffsetBean]

  /**
    * 获取历史数据记录并执行校验，防止部分实现类出现逻辑异常
    * 校验规则为:使用 groupId+TopicPar 作为唯一 key 判断数据是否重复
    */
  @throws[ConsumerOffsetHandlerException]
  @throws[IllegalAccessException]
  def fetchCheckIllegal(groupId: String, topicsSet: Set[String]): Map[TopicPar, ConsumerOffsetBean] = {
    val beans: Seq[ConsumerOffsetBean] = this.fetch(groupId, topicsSet)
    if (beans == null || beans.isEmpty) return Map()
    val originalSize: Int = beans.size // 原始数据大小
    val resultSize: Int = beans.toSet.size // 去重后结果数据大小
    if (originalSize != resultSize) {
      throw new IllegalAccessException(
        s"consumerOffset data illegal , originalSize diff $originalSize != $resultSize")
    }
    // object->json
    implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)
    logWarn(s"fetch beans:${Serialization.write(beans)}")
    beans.map({ a => a.topicPar -> a }).toMap
  }

  /**
    * replaceInto.
    * @param beans the consumer offsets
    * @return int[] 批次中更新的行数.
    */
  @Nullable
  @throws[ConsumerOffsetHandlerException]
  def replaceInto(beans: Seq[ConsumerOffsetBean]): Array[Int]
}


/** 第三方偏移量记录消费操作接口异常 */
class ConsumerOffsetHandlerException(s: String, e: Exception) extends Exception {}


/** 消费策略 */
object ConsumptionStrategy {

  /** 根据消费策略提取对应偏移量 */
  private[kafka] def fetchOffset(strategy: String, b: TopicParOffsetBean): Long = strategy match {
    case "BEGINNING_OFFSET" => b.begOffset
    case "END_OFFSET" => b.endOffset
    case _ => throw new IllegalStateException("Unexpected value: " + strategy)
  }
}


/** TopicPar 功能类似[[TopicPartition]]，支持排序  */
private[kafka] case class TopicPar(topic: String, partition: Int) extends Ordered[TopicPar] {
  override def compare(that: TopicPar): Int = {
    if (this.topic == that.topic) {
      this.partition - that.partition
    } else {
      this.topic.compareTo(that.topic)
    }
  }
}

/** 历史消费记录 */
private[kafka] case class ConsumerOffsetBean(groupId: String, topicPar: TopicPar, fromOffset: Long, untilOffset: Long,
                                             lastModified: String =
                                             new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()))
  extends Ordered[ConsumerOffsetBean] {
  /** Number of messages this OffsetRange refers to */
  def count(): Long = untilOffset - fromOffset

  override def compare(that: ConsumerOffsetBean): Int = this.topicPar.compare(that.topicPar)
}

/** kafka 主题-分区下对应消息的最早及最晚偏移量 */
private[kafka] case class TopicParOffsetBean(topicPar: TopicPar, begOffset: Long, endOffset: Long)
  extends Ordered[TopicParOffsetBean] {
  override def compare(that: TopicParOffsetBean): Int = this.topicPar.compare(that.topicPar)
}
