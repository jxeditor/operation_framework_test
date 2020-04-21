package com.frame.test.batch

import java.util

import com.frame.tools.time.TimesDtTools
import com.sun.org.glassfish.gmbal.Description
import org.apache.flink.table.api.{EnvironmentSettings, SqlDialect, Table, TableEnvironment, TableUtils}
import org.apache.flink.table.catalog.hive.HiveCatalog
import org.apache.flink.table.functions.ScalarFunction
import org.junit.{Assert, Test}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.GlobalConfiguration
import org.apache.flink.table.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.config.{ExecutionConfigOptions, OptimizerConfigOptions}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * @author XiaShuai on 2020/4/20.
  */
@Test
class GameBusinessTest extends Assert {

  val dt = "2019-08-16"
  val appId = "game_skuld_01"
  private val beforeDt = TimesDtTools.dt(dt, -1)
  private val lastWeekMonday = TimesDtTools.monday(dt, -1)
  private val thisWeekMonday = TimesDtTools.monday(dt, 0)
  private val lastMonthFirst = TimesDtTools.dtMonthFirstDay(dt, -1)
  private val thisMonthFirst = TimesDtTools.dtMonthFirstDay(dt, 0)

  val env = ExecutionEnvironment.getExecutionEnvironment
  val btEnv = BatchTableEnvironment.create(env)
  val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build()
  val tEnv = TableEnvironment.create(settings)
  //  val env = StreamExecutionEnvironment.getExecutionEnvironment
  //  val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build
  //  val tEnv = StreamTableEnvironment.create(env, settings)

  tEnv.getConfig.getConfiguration.setBoolean(OptimizerConfigOptions.TABLE_OPTIMIZER_JOIN_REORDER_ENABLED, true)
  tEnv.getConfig.getConfiguration.setBoolean(OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SOURCE_ENABLED, false)
  tEnv.getConfig.getConfiguration.setLong(OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD, 10485760L)
  tEnv.getConfig.getConfiguration.setInteger(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1)
  tEnv.getConfig.getConfiguration.setInteger(ExecutionConfigOptions.TABLE_EXEC_SORT_DEFAULT_LIMIT, 200)
  tEnv.getConfig.addConfiguration(GlobalConfiguration.loadConfiguration)

  //  val hiveCatalog = new HiveCatalog("catalog", "default",
  //    "F:\\operation_framework_test\\flink\\src\\main\\resources\\hive_conf", "2.1.1")
  //  hiveCatalog.getHiveConf.set("dfs.client.use.datanode.hostname", "true")
  //  tEnv.getConfig.setSqlDialect(SqlDialect.HIVE)
  //  tEnv.registerCatalog("catalog", hiveCatalog)
  //  tEnv.useCatalog("catalog")
  //
  //  btEnv.getConfig.setSqlDialect(SqlDialect.HIVE)
  //  btEnv.registerCatalog("catalog", hiveCatalog)
  //  btEnv.useCatalog("catalog")

  tEnv.registerFunction("udfMapFilterNotKeyPre", new MapFilterNotKeyPreUdf)

  @Test
  @Description("ODS->DWA(最后登录数据)")
  def dwaLoginBase(): Unit = {
    val table = tEnv.sqlQuery(
      s"""
         |SELECT *, ROW_NUMBER() OVER (PARTITION BY rid ORDER BY data_unix DESC) AS num
         |FROM  game_ods.event
         |WHERE app = 'game_skuld_01'
         |  AND dt = '2019-08-16'
         |  AND event = 'event_role.login_1'
         |""".stripMargin
    ).filter(
      "num = 1"
    ).select(
      "udfMapFilterNotKeyPre('r_eco_',p) as p, id, platform, channel, region, server, data_unix, uid, rid, did"
    )

    table.printSchema()

    val rows = TableUtils.collectToList(table)
    rows.toArray().foreach(println)

    tEnv.sqlUpdate(
      s"""
         |insert into test
         |SELECT rid
         |FROM $table
         |""".stripMargin)
    tEnv.execute("")
  }

  @Test
  @Description("ODS->DWA(首次活跃数据)")
  def dwaActiveBase(): Unit = {
    val table = tEnv.sqlQuery(
      s"""
         |SELECT rid, data_unix, channel, server, ROW_NUMBER() OVER (PARTITION BY rid ORDER BY data_unix) AS num
         |FROM  game_ods.event
         |WHERE app = 'game_skuld_01'
         |  AND dt = '2019-08-16'
         |  AND event = 'event_role.login_1'
         |""".stripMargin
    ).filter(
      "num = 1"
    ).select(
      "rid, data_unix, channel, server"
    )

    val rows = TableUtils.collectToList(table)
    rows.toArray().foreach(println)

    tEnv.sqlUpdate(
      s"""
         |insert into test
         |SELECT rid
         |FROM $table
         |""".stripMargin)
    tEnv.execute("")
  }

  @Test
  @Description("ODS->DWA(用户付费数据)")
  def dwaPayBase(): Unit = {
    val table: Table = tEnv.sqlQuery(
      s"""
         |SELECT rid, cast(p['t_t4_d'] as int) as pay
         |FROM  game_ods.event
         |WHERE app   = 'game_skuld_01'
         |  AND dt    = '2019-08-16'
         |  AND event = 'event_role.pay_3_d'
         |""".stripMargin
    )

    val rows = TableUtils.collectToList(table).toArray().map(x => {
      val row = x.asInstanceOf[Row]
      (row.getField(0).toString, row.getField(1).toString.toInt, 1)
    })
    val result = env.fromCollection(rows)
      .groupBy(0).sum(1)
      .groupBy(0).sum(2)

    val table1 = btEnv.fromDataSet(result, 'rid, 'total, 'times)

    table1.select('rid).insertInto("test")
    btEnv.execute("")
  }

  @Test
  @Description("DWA->DWS(用户周期活跃数据)")
  def dwsActiveContinue(): Unit = {
    val active = tEnv.sqlQuery(
      s"""
         |SELECT tid
         |FROM game_dwa.data_active_simple_fs
         |WHERE app   = 'game_skuld_01'
         |  AND dt    = '2019-08-16'
         |  AND event = 'u_login'
         |""".stripMargin)

    val activeDay = tEnv.sqlQuery(
      s"""
         |SELECT tid, day_times, ROW_NUMBER() OVER (PARTITION BY tid ORDER BY day_times DESC) AS num
         |FROM  game_dws.data_active_continue_cycle
         |WHERE app   = 'game_skuld_01'
         |  AND event  = 'u_active'
         |  AND cycle = 'day'
         |  AND dt = '2019-08-15'
         |""".stripMargin).filter("num = 1")

    val activeWeek = tEnv.sqlQuery(
      s"""
         |SELECT tid, week_times, ROW_NUMBER() OVER (PARTITION BY tid ORDER BY week_times DESC) AS num
         |FROM  game_dws.data_active_continue_cycle
         |WHERE app   = 'game_skuld_01'
         |  AND event  = 'u_active'
         |  AND cycle = 'day'
         |  AND dt >= '$lastWeekMonday'
         |  AND dt < '$thisWeekMonday'
         |""".stripMargin).filter("num = 1")

    val activeMonth = tEnv.sqlQuery(
      s"""
         |SELECT tid, month_times, ROW_NUMBER() OVER (PARTITION BY tid ORDER BY month_times DESC) AS num
         |FROM  game_dws.data_active_continue_cycle
         |WHERE app   = 'game_skuld_01'
         |  AND event  = 'u_active'
         |  AND cycle = 'day'
         |  AND dt >= '$lastMonthFirst'
         |  AND dt < '$thisMonthFirst'
         |""".stripMargin).filter("num = 1")

    active.leftOuterJoin(
      activeDay, "active.tid = activeDay.tid"
    ).leftOuterJoin(
      activeWeek, "active.tid = activeWeek.tid"
    ).leftOuterJoin(
      activeMonth, "active.tid = activeMonth.tid"
    ).select(
      s"""
         |active.tid,
         |if(activeDay.tid is null, 1, cast(activeDay.day_times as int) + 1),
         |if(activeWeek.tid is null, 1, cast(activeWeek.day_times as int) + 1),
         |if(activeMonth.tid is null, 1, cast(activeMonth.day_times as int) + 1),
         |""".stripMargin)

    activeDay.printSchema()

    tEnv.execute("")
  }

  @Test
  @Description("DWA->DWS(用户付费类型数据)")
  def dwsPayType(): Unit = {
    val allPay = tEnv.sqlQuery(
      s"""
         |SELECT id, rid, data_unix, ROW_NUMBER() OVER (PARTITION BY rid ORDER BY data_unix) num
         |FROM game_ods.event
         |WHERE app   = '$appId'
         |  AND dt    = '$dt'
         |  AND event = 'event_role.pay_3_d'
         |""".stripMargin).filter("num <= 3")

    val firstHistory = tEnv.sqlQuery(
      s"""
         |SELECT rid, ROW_NUMBER() OVER (PARTITION BY rid ORDER BY data_unix) num
         |FROM game_dws.data_pay_type_cycle
         |WHERE app   = '$appId'
         |  AND event  = 'r_pay'
         |  AND cycle = 'day'
         |  AND dt < '$dt'
         |  AND times = 'first'
         |""".stripMargin).filter("num = 1")

    val fsTmp = allPay.leftOuterJoin(
      firstHistory, "allPay.rid = firstHistory.rid"
    ).select(
      s"""
         |allPay.id, allPay.rid, allPay.data_unix, firstHistory.rid history,
         |ROW_NUMBER() OVER (PARTITION BY allPay.rid ORDER BY allPay.data_unix) num
         |""".stripMargin
    )

    val firstPay = fsTmp.filter("num = 1 AND history is null")

    tEnv.execute("")
  }

  @Test
  @Description("DWA->DWS(新增类型指标分析)")
  def dwsRegister(): Unit = {
    val today = tEnv.sqlQuery(
      s"""
         |SELECT did,data_unix,channel,server
         |FROM game_dwa.data_active_simple_fs
         |WHERE app  = '$appId'
         |  AND dt   = '$dt'
         |  AND event = 'd_login'
         |""".stripMargin)

    val history = tEnv.sqlQuery(
      s"""
         |SELECT did, ROW_NUMBER() OVER (PARTITION BY did ORDER BY data_unix) num
         |FROM game_dws.data_register_cycle
         |WHERE app   = '$appId'
         |  AND event  = 'd_register'
         |  AND cycle = 'day'
         |  AND dt < '$dt'
         |""".stripMargin).filter("num = 1")

    val result = today.leftOuterJoin(history, "today.did = history.did").filter("history.did is null")
    tEnv.sqlUpdate(
      s"""
         |insert overwrite test
         |select *
         |from $result
         |""".stripMargin)
    tEnv.execute("")
  }

  @Test
  @Description("DWS->ADS(活跃基础详情数据)")
  def adsActiveBase(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build
    val tEnv = StreamTableEnvironment.create(env, settings)
    val table1 = tEnv.sqlQuery(s"select 1002 as id, 1587378463 as unix, 'LOGIN' as type")
    val table2 = tEnv.sqlQuery(s"select 1002 as id, 1587388463 as unix, 'LOGOUT' as type")

    val table = table1.unionAll(table2)

    val table3 = tEnv.sqlQuery(
      s"""
         |select id, cast(unix as string) as unix, cast(type as string) as type
         |from $table
         |""".stripMargin)

    val table4 = tEnv.sqlQuery(
      s"""
         |select cast(id as string) id, listagg(type) as str
         |from $table3
         |group by id
         |""".stripMargin)

    //    val rows = TableUtils.collectToList(table4)
    //    rows.toArray().foreach(println)

    tEnv.sqlUpdate(createEsTable())
    tEnv.sqlUpdate(
      s"""
         |insert into test_xs_01
         |select str
         |from $table4
         |group by str
         |""".stripMargin)

    tEnv.execute("")
  }


  def createKafkaTable(): String = {
    """
      |CREATE TABLE kafka_test (
      |    rid VARCHAR COMMENT 'rid',
      |    times BIGINT NOT NULL,
      |    total INTEGER
      |)
      |WITH (
      |    'connector.type' = 'kafka', -- 使用 kafka connector
      |    'connector.version' = 'universal',  -- kafka 版本
      |    'connector.topic' = 'test01',  -- kafka topic
      |    'connector.properties.0.key' = 'zookeeper.connect',  -- zk连接信息
      |    'connector.properties.0.value' = 'skuldcdhtest1.ktcs:2181',  -- zk连接信息
      |    'connector.properties.1.key' = 'bootstrap.servers',  -- broker连接信息
      |    'connector.properties.1.value' = 'skuldcdhtest1.ktcs:9092',  -- broker连接信息
      |    'connector.sink-partitioner' = 'fixed',
      |    'update-mode' = 'append',
      |    'format.type' = 'json',  -- 数据源格式为 json
      |    'format.derive-schema' = 'true' -- 从 DDL schema 确定 json 解析规则
      |)
    """.stripMargin
  }

  def createEsTable(): String = {
    s"""
       |CREATE TABLE test_xs_01 (
       | id VARCHAR
       |) WITH (
       | 'connector.type' = 'elasticsearch',
       | 'connector.version' = '7',
       | 'connector.hosts' = 'http://skuldcdhtest1.ktcs:9200;http://skuldcdhtest2.ktcs:9200;http://skuldcdhtest3.ktcs:9200',
       | 'connector.index' = 'test_xs_01',
       | 'connector.document-type' = 'test_xs_01',
       | 'update-mode' = 'upsert',
       | 'format.type' = 'json'
       |)
       |""".stripMargin
  }
}


class MapFilterNotKeyPreUdf extends ScalarFunction {
  def eval(fieldPre: String, map: java.util.Map[String, String]): mutable.Map[String, String] = {
    map.asScala.filterNot(_._1.startsWith(fieldPre))
  }
}

class OnlineTimeUdf extends ScalarFunction {
  val dt = "2019-08-16"
  val f_login = "LOGIN"
  val f_logout = "LOGOUT"
  val dtUnix: Int = TimesDtTools.unixSec(dt).toInt
  val dtEndUnix: Int = TimesDtTools.unixSec(TimesDtTools.dt(dt, 1)).toInt

  def eval(list: Seq[String]): Int = {
    val loginArr = list.map(x => {
      val array = x.split(",")
      (array(0), array(1).toInt)
    }).sortBy(_._2)

    var cost = 0
    var lastType = f_logout
    var lastUnix = dtUnix
    for (i <- loginArr.indices) {
      val currType = loginArr(i)._1
      val currUnix = loginArr(i)._2
      if (!(lastType == f_logout && currType == f_login)) {
        cost += (currUnix - lastUnix)
      }
      lastType = currType
      lastUnix = currUnix
    }
    if (lastType == f_login) {
      cost += dtEndUnix - lastUnix
    }
    cost
  }
}


