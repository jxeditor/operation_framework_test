package com.frame.job

import com.frame.core.Job
import com.frame.core.exception.JobException
import com.frame.job.PredefSparkJob.Session
import com.frame.log.Logging
import com.frame.tools.conf.Conf
import com.frame.tools.convert.FastJsonConvert
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import com.frame.constant.AnalystSparkConfConstant.SK_SPARK_APP_NAME
import com.frame.utils.spark.SparkUdf

/**
  * @author XiaShuai on 2020/4/8.
  */
abstract class SparkJob(implicit val conf: Conf)
  extends Job with Logging with FastJsonConvert {

  private val _sparkConf: SparkConf = buildSparkConf()
  private val _sc: SparkContext = SparkContext.getOrCreate(_sparkConf)
  private val _sSession: Session = SparkSession.builder.config(_sparkConf).enableHiveSupport.getOrCreate
  private val _default_ui_port = 10101

  @throws[JobException]
  override final def execute(): Unit = {
    try {
      SparkUdf.udfRegisterAll() // 注册 udf 函数
      open()
      jobCompute()
    } catch {
      case e: Exception =>
        throw new JobException("jobCompute error , Caused by " + e.getMessage, e)
    } finally {
      try
        this.close()
      catch {
        case _: Exception =>
      }
    }
  }

  /** 执行前打开操作，常用于资源初始化 */
  def open(): Unit = {}

  override def close(): Unit = {
    if (sparkSession != null) try
      sparkSession.stop()
    catch {
      case _: Exception =>
    }
  }

  implicit def sparkSession: Session = _sSession

  /** 任务类型 */
  def jobType: String

  def sparkContext: SparkContext = _sc

  def buildSparkConf(): SparkConf = {
    // 判断是否配置 skuld.spark.app.name，未配置默认使用实现类类名
    new SparkConf().setAppName(
      conf.getString(SK_SPARK_APP_NAME, this.getClass.getSimpleName))
      .set("spark.ui.port", sparkUiPort().toString)
      .set("spark.port.maxRetries", if (_default_ui_port == sparkUiPort()) "128" else "0") // 指定端口时任务不进行端口+1重试
      .set("spark.network.timeout", "360s")
      .set("fs.hdfs.impl.disable.cache", "true")
      .set("spark.port.maxRetries", "100")
      .set("spark.driver.maxResultSize", "10g")
      .set("spark.scheduler.listenerbus.eventqueue.capacity", "100000")
      // Truncated the string representation of a plan since it was too large.
      // This behavior can be adjusted by setting 'spark.debug.maxToStringFields' in SparkEnv.conf
      .set("spark.debug.maxToStringFields", "100")

      /** spark-sql 参数 */
      .set("spark.sql.result.partitions", "1")
      .set("spark.sql.hive.inputformat", "org.apache.hadoop.mapred.lib.CombineTextInputFormat")
      // Evicting cached table partition metadata from memory due to size constraints
      // (spark.sql.hive.filesourcePartitionFileCacheSize = 262144000 bytes).
      // This may impact query planning performance.
      .set("spark.sql.hive.filesourcePartitionFileCacheSize", (500 * 1024 * 1024).toString)
      .set("spark.sql.inMemoryColumnarStorage.compressed", "true") // Spark SQL将根据数据统计信息自动为每列选择压缩编解码器
      .set("spark.sql.inMemoryColumnarStorage.batchSize", "10000")
      .set("spark.sql.files.maxPartitionByte", "134217728") // 134217728 (128 MB) 读取文件时打包到单个分区的最大字节数
      .set("spark.sql.files.openCostInBytes", "4194304") // 4194304 (4 MB)
      .set("spark.sql.shuffle.partitions", "120") // 根据资源情况实际调整，目前资源较小采用 120。默认值 200
      .set("spark.sql.broadcastTimeout", "36000")
      // 付费类类任务报错：Either: use the CROSS JOIN syntax to allow cartesian products between these
      .set("spark.sql.crossJoin.enabled", "true")
      .set("spark.sql.autoBroadcastJoinThreshold", (15L * 1024 * 1024).toString) // 广播域值 15 M

      /** spark-hive 参数 */
      .set("hive.exec.dynamic.partition", "true")
      .set("hive.exec.dynamic.partition.mode", "nonstrict")
      .set("hive.merge.sparkfiles", "true")
      .set("hive.merge.mapfiles", "true")
      .set("hive.merge.mapredfiles", "true")
      .set("hive.merge.size.per.task", "256000000")
      .set("hive.merge.smallfiles.avgsize", "256000000")
      .set("hive.groupby.skewindata", "true")

      /** spark-kryo 序列化相关配置 */
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrationRequired", "false")
      .registerKryoClasses(this.kryoRegistratorClass.toArray)
  }

  /** 默认从 [[SparkJob._default_ui_port]] 开始占用端口, 赋予特殊需要监控的任务重写该方法 */
  def sparkUiPort(): Int = _default_ui_port

  /**
    * kryo 序列化类,根据实际任务需要进行重写该方法，默认为空,
    * 如果对于任务中需要支持kryo序列化类未知，请开启 spark.kryo.registrationRequired 执行任务后查看异常一一添加
    */
  def kryoRegistratorClass: Seq[Class[_]] = Seq()

  @throws[JobException]
  def jobCompute(): Unit

}
