package com.frame.test.batch

import java.util

import com.sun.org.glassfish.gmbal.Description
import org.apache.flink.table.api.{EnvironmentSettings, SqlDialect, Table, TableEnvironment, TableUtils}
import org.apache.flink.table.catalog.hive.HiveCatalog
import org.apache.flink.table.delegation.PlannerExpressionParser
import org.apache.flink.table.expressions.{Avg, Expression, ExpressionParser}
import org.apache.flink.table.functions.ScalarFunction
import org.junit.{Assert, Test}
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.parallel.immutable

/**
  * @author XiaShuai on 2020/4/20.
  */
@Test
class GameBusinessTest extends Assert {
  val env = ExecutionEnvironment.getExecutionEnvironment
  val btEnv = BatchTableEnvironment.create(env)
  val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build()
  val tEnv = TableEnvironment.create(settings)
  //  val env = StreamExecutionEnvironment.getExecutionEnvironment
  //  val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build
  //  val tEnv = StreamTableEnvironment.create(env, settings)
  val hiveCatalog = new HiveCatalog("catalog", "default",
    "F:\\operation_framework_test\\flink\\src\\main\\resources\\hive_conf", "2.1.1")
  hiveCatalog.getHiveConf.set("dfs.client.use.datanode.hostname", "true")
  tEnv.getConfig.setSqlDialect(SqlDialect.HIVE)
  tEnv.registerCatalog("catalog", hiveCatalog)
  tEnv.useCatalog("catalog")

  btEnv.getConfig.setSqlDialect(SqlDialect.HIVE)
  btEnv.registerCatalog("catalog", hiveCatalog)
  btEnv.useCatalog("catalog")

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
}


class MapFilterNotKeyPreUdf extends ScalarFunction {
  def eval(fieldPre: String, map: java.util.Map[String, String]): mutable.Map[String, String] = {
    map.asScala.filterNot(_._1.startsWith(fieldPre))
  }
}


