package com.frame.utils.spark

import com.frame.constant.AnalystSparkConfConstant._
import com.frame.core.IOptions
import com.frame.job.PredefSparkJob.Session
import com.frame.log.Logging
import com.frame.tools.time.TimesDtTools
import com.frame.utils.spark.SparkJobUtil.{logWarn, noNumPartitions}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ContentSummary, FileSystem, Path}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.bson.types.ObjectId

/**
  * @author XiaShuai on 2020/4/8.
  */
object SparkJobUtil extends Logging {
  /** 常量定义 */
  val noNumPartitions: Int = -1
  val oneNumPartitions: Int = 1
  val defaultNumPartitions: Int = 20

  /* ------------------------------------------------------------------------------------- *
       HDFS 路径相关操作方法
   * ------------------------------------------------------------------------------------- */

  def buildOdsHdfsPath(app: String, dt: String, event: String): String =
    s"/user/hive/warehouse/game_ods.db/event/app=$app/dt=$dt/event=$event/"

  /** 正则匹配 {dt=a,dt=b} e.g .../app=game_skuld_01/{dt=2019-07-07,dt=2019-07-08,dt=2019-07-09}/event=... */
  def buildOdsHdfsPath(app: String, dt: Set[String], event: String): String =
    s"/user/hive/warehouse/game_ods.db/event/app=$app/${dt.map("dt=" + _).mkString("{", ",", "}")}/event=$event/"

  /* ------------------------------------------------------------------------------------- *
       HIVE 分区 相关操作方法
   * ------------------------------------------------------------------------------------- */

  def alertHivePartition(table: String, app: String, dt: String, event: String)(implicit session: Session): Unit =
    executeHiveSql(s"ALTER TABLE $table ADD IF NOT EXISTS PARTITION (app='$app',dt='$dt',event='$event')")

  /*
  压缩前
  525.1 M  1.0 G /user/hive/warehouse/game_ods.db/event/app=game_skuld_01/dt=2019-07-14/event=event_role.activity_4/
  压缩后
  1.2 G  2.5 G  /user/hive/warehouse/game_ods.db/event/app=game_skuld_01/dt=2019-07-14/event=event_role.activity_4
   */
  /** 根据给定目录数据量计算分区数 如果目录不存在或者数据大小为 0 ， 返回 0 否则最小为 1 */
  def pathRepartition(filePath: String): Int = {
    val blockLength: Double = 1024 * 1024 * 128D // 128MB
    val path = new Path(filePath)
    val fs: FileSystem = path getFileSystem new Configuration()
    var r = 0
    if (fs.exists(path)) {
      val summary: ContentSummary = fs.getContentSummary(path)
      val length = summary.getLength
      if (length > 0) {
        val blockCount: Double = length / blockLength
        r = Math.max(if (blockCount < 0.7) 1 else blockCount * 1.4, 1).toInt // 数据目录下大小不为 0 时 , 默认最少 1 个分区
      }
      logWarn(s"pathRepartition [$filePath] ,repartition=[$r] ,summary=[${summary.toString(false, true)}]")
    }
    r
  }

  /* ------------------------------------------------------------------------------------- *
       sql 相关操作方法
   * ------------------------------------------------------------------------------------- */

  /**
    * 根据传入的基准日期和计算数组，拼接成("2019-01-01","2019-01-02")的形式
    * @param dt         计算的基准日期
    * @param calculated 计算拼接日期的数组
    * @return
    */
  def connectDts(dt: String, calculated: Array[Int]): String = {
    var dts = "("
    var connect = ","
    for (day <- calculated) {
      val calculate = TimesDtTools.dt(dt, -day)
      if (day == calculated(calculated.length - 1)) connect = ")"
      dts = s"$dts '${calculate.toString}' $connect"
    }
    dts
  }

  def connectDtsCondition(dtField: String, dt: String, calculated: Array[Int]): String = {
    val builder = StringBuilder.newBuilder
    builder.append("(")
    for (elem <- calculated) {
      builder.append(s"$dtField='${TimesDtTools.dt(dt, -elem)}' OR ")
    }
    builder.append("1!=1)")

    builder.toString()
  }

  /**
    * 拼接sql where
    * eg: whereCondition("and",whereCondition("=","appId","game_skuld_01")(),whereCondition("=","dt","20190512")(""))()
    * @param connect 连接符
    * @param filed   字段
    * @param values  字段值
    * @param link    多个where之间的连接服
    * @return where 语句。eg:(dt=20190501) or (dt=20190502) 、 appId='game_skuld' and dt=20190512
    */
  def whereCondition(connect: String, filed: String, values: String*)(link: String = " "): String = {
    var whereCondition = ""
    val leftBrace = " ( "
    val rightBrace = " ) "
    for (value <- values) {
      // 拼接小的where eg:(dt=20190501)
      val condition = leftBrace + filed + connect + value + rightBrace
      // 连接小where  eg:(dt=20190501) or
      whereCondition += condition + link
    }
    whereCondition.substring(0, whereCondition.lastIndexOf(link))
  }

  /**
    * 读取多条 SQL&HIVE 表数据注册为临时表，重新分区后写入 HIVE 表
    * 此方法主要针对 Spark-SQL-Hive 执行多条 sql 合并结果, insert 后生成大量小文件进行合并处理
    * @see [[RwSqlOptions]]
    */
  def writeHive(rwOptions: RwSqlOptions)(implicit session: Session): Unit = {
    val frames = Seq.newBuilder[DataFrame]
      .++=(rwOptions.rSql.map(executeHiveSql(_, rwOptions.wRepartition)))
      .++=(rwOptions.rDataSet)
      .++=(rwOptions.rDataFrame)
    val dataFrames = frames.result()

    if (dataFrames.nonEmpty) { // 非空判断
      def insert(frame: DataFrame): Unit = {
        val rOptions = RSqlOptions(dataSet = frame, wRepartition = rwOptions.wRepartition)
        val r = this.readTuple2(rOptions)
        val sql = s"${rwOptions.wSql} SELECT * FROM ${r._1}"
        this.executeHiveSql(sql)
      }

      if (rwOptions.splitExecute) { // 拆分写入
        dataFrames.foreach(insert)
      } else { // 合并写入
        var frame = dataFrames.head
        for (i <- 1.until(dataFrames.length)) {
          frame = frame.union(dataFrames(i))
        }
        insert(frame)
      }
    }
  }

  /** 读取 hive 数据重置分区返回 DataFrame */
  def executeHiveSql(sql: String, numPartitions: Int = noNumPartitions)(implicit session: Session): DataFrame = {
    val df = session.sql(sql)
    var partitions = numPartitions
    if (sql.contains("game_skuld_0")) { // 运行 demo 游戏调试信息输出
      // 运行 demo 游戏时分区默认 1 时更新为 3
      partitions = if (partitions == oneNumPartitions) oneNumPartitions * 3 else partitions
      // 目前测试仅在 game_skuld_01 sql 中打印执行计划
      if (getConf.getBoolean(SK_SPARK_SQL_PRINT_EXPLAIN) && sql.contains("game_skuld_01")) {
        if (sql.length > 1000) { // SQL 长度大于阈值不进行打印
          logWarn(s"exeSql#numPartitions=$partitions#sql.length>3000, substring, sql:\n${sql.substring(0, 1000)}")
        } else {
          logWarn(s"exeSql#numPartitions=$partitions#sql:\n$sql")
        }
        df.explain()
      }
    }
    this.repartition(df, partitions)
  }

  /** 重置分区数 , numPartitions <= 0 表示不进行 repartition()操作 */
  private def repartition(frame: DataFrame, numPartitions: Int): DataFrame = {
    if (numPartitions >= oneNumPartitions) {
      if (numPartitions == Integer.MAX_VALUE) {
        val l: Long = frame.count().longValue()
        val dynamicNumPartitions = ((l / 1000000L) + 1).asInstanceOf[Int]
        frame.repartition(dynamicNumPartitions)
      } else {
        frame.repartition(numPartitions)
      }
    } else {
      frame
    }
  }

  /**
    * DataSet[Row] 注册临时表,具体支持操作参考 [[RSqlOptions]]
    * @param rOptions rOptions
    * @param session  session
    * @return (df 临时表名，df)
    */
  def readTuple2(rOptions: RSqlOptions)(implicit session: Session): (String, DataFrame) = {
    var newTempViewName: String = rOptions.tempViewName
    if (newTempViewName == null) {
      newTempViewName = this.fetchUniqueTempTableName
    }
    val reFrame = this.repartition(rOptions.dataSet, rOptions.wRepartition)
    reFrame.createOrReplaceTempView(newTempViewName)
    Tuple2.apply(newTempViewName, reFrame)
  }

  /** @return 唯一临时表名 */
  private def fetchUniqueTempTableName: String = "temp_" + ObjectId.get.toHexString.replace('-', '_')

  /**
    * 读取 sql 转换成df
    * @param sql           sql
    * @param numPartitions 重置分区数
    * @param session       session
    * @return (df 临时表名，df)
    */
  def readTuple2(sql: String, numPartitions: Int = noNumPartitions)(implicit session: Session): (String, DataFrame) = {
    readTuple2(RSqlOptions(executeHiveSql(sql), wRepartition = noNumPartitions))
  }

}

/**
  * 读写操作相关设值
  * @param wSql         写 sql，多条情况下将合并为一个 df , e.g INSERT OVERWRITE TABLE TB-NAME PARTITION(app_id, dt)
  * @param rDataSet     待写入DataFrame, 多个DataFrame将合并为一个DataFrame
  * @param splitExecute 是否拆分执行
  * @param wRepartition 写入时重置分区数量，默认为-1 不进行重置操作
  */
case class RwSqlOptions(rSql: Seq[String] = Seq[String](),
                        rDataSet: Seq[Dataset[Row]] = Seq[Dataset[Row]](),
                        rDataFrame: Seq[DataFrame] = Seq[DataFrame](),
                        wSql: String,
                        splitExecute: Boolean = false,
                        wRepartition: Int = noNumPartitions)
  extends IOptions {
}

/**
  * 读操作相关设值
  * @param dataSet      数据集
  * @param tempViewName 临时表名，为空时自动生成
  * @param wRepartition 写入时重置分区数量，默认为-1 不进行重置操作
  */
case class RSqlOptions(dataSet: Dataset[Row], tempViewName: String = null, wRepartition: Int = noNumPartitions)
  extends IOptions {}

object SparkUdf {

  /* ------------------------------------------------------------------------------------- *
     register.udf 函数相关操作方法
   * ------------------------------------------------------------------------------------- */

  val udfMapFilterKeyPre = "udfMapFilterKeyPre"
  val udfMapFilterNotKeyPre = "udfMapFilterNotKeyPre"
  val udfMapCombine = "udfMapCombine"

  def udfRegisterAll()(implicit session: Session): Unit = {
    udfRegisterMapFilterKeyPre()
    udfRegisterMapFilterNotKeyPre()
    udfRegisterMapCombine()
  }

  /** 过滤出map对象中给定前缀的字段 */
  def udfRegisterMapFilterKeyPre()(implicit session: Session): Unit = {
    logWarn(s"session.sqlContext.udf.register[$udfMapFilterKeyPre]")
    session.sqlContext.udf.register(
      udfMapFilterKeyPre,
      (map: Map[String, String], fieldPre: Seq[String]) => {
        map.filter(f => {
          val index = fieldPre.indexWhere(field => f._1.startsWith(field))
          if (index >= 0) true
          else false
        })
      })
  }

  /** 过滤出map对象中不是给定前缀的字段 */
  def udfRegisterMapFilterNotKeyPre()(implicit session: Session): Unit = {
    logWarn(s"session.sqlContext.udf.register[$udfMapFilterNotKeyPre]")
    session.sqlContext.udf.register(
      udfMapFilterNotKeyPre,
      (fieldPre: String, map: Map[String, String]) => map.filterNot(_._1.startsWith(fieldPre))
    )
  }

  /**
    * 将多个map进行合并
    */
  def udfRegisterMapCombine()(implicit session: Session): Unit = {
    logWarn(s"session.sqlContext.udf.register[$udfMapCombine]")
    session.sqlContext.udf.register(
      udfMapCombine,
      (maps: Seq[Map[String, String]]) => {
        val r = Map.canBuildFrom[String, String]()
        maps.filter(_ != null).foreach(f => r.++=(f))
        r.result()
      }
    )
  }

}

