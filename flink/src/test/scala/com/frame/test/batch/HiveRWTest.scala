package com.frame.test.batch

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.junit.{Assert, Test}
import org.apache.flink.table.api.{EnvironmentSettings, SqlDialect, TableEnvironment, TableSchema, TableUtils}
import org.apache.flink.table.catalog.ObjectPath
import org.apache.flink.table.catalog.hive.HiveCatalog

/**
  * https://ververica.cn/developers/flink-batch-sql-110/
  * @author XiaShuai on 2020/4/8.
  */
@Test
class HiveRWTest extends Assert {

  @Test
  def readHiveTest(): Unit = {
    // System.setProperty("hadoop.home.dir", "E:\\Soft\\hadoop-2.8.0")
    //    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build
    //    val tEnv = StreamTableEnvironment.create(env, settings)
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build()
    val tEnv = TableEnvironment.create(settings)


    val hiveCatalog = new HiveCatalog("test", "default",
      "F:\\operation_framework_test\\flink\\src\\main\\resources\\hive_conf", "2.1.1")
    hiveCatalog.getHiveConf.set("dfs.client.use.datanode.hostname", "true")
    tEnv.registerCatalog("test", hiveCatalog)
    tEnv.useCatalog("test")
    //    tEnv.sqlUpdate(createMysqlTable())
    tEnv.sqlUpdate(createKafkaTable())
    tEnv.listTables().foreach(println)

    // 当结果表为Hive表时
    //    tEnv.getConfig.setSqlDialect(SqlDialect.HIVE)

    //    val table = tEnv.sqlQuery(
    //      s"""
    //         |SELECT uid,rid
    //         |FROM (
    //         |  SELECT *, ROW_NUMBER() OVER (PARTITION BY rid ORDER BY data_unix ASC) AS num
    //         |  FROM  game_ods.event
    //         |  WHERE app = 'game_skuld_01'
    //         |    AND dt = '2019-08-16'
    //         |    AND event = 'event_role.login_1'
    //         |) tmp
    //         |WHERE tmp.num = 1
    //         |""".stripMargin)
    //    val rows = TableUtils.collectToList(table)
    //    println(rows)

    val table = tEnv.sqlQuery(
      s"""
         |SELECT *, ROW_NUMBER() OVER (PARTITION BY rid ORDER BY data_unix DESC) AS num
         |FROM  game_ods.event
         |WHERE app = 'game_skuld_01'
         |  AND dt = '2019-08-16'
         |  AND event = 'event_role.login_1'
         |""".stripMargin
    ).filter("num = 1").select(
      "uid,rid"
    ).groupBy(
      "uid,rid"
    ).select("uid,rid")

    tEnv.sqlUpdate(
      s"""
         |insert into demo1
         |SELECT uid,rid
         |FROM $table
         |""".stripMargin)
    tEnv.execute("")
  }

  def createMysqlTable(): String = {
    "create table demo (" +
      "`uid` VARCHAR," +
      "`rid` VARCHAR" +
      ") with (" +
      " 'connector.type' = 'jdbc', " +
      " 'connector.url' = 'jdbc:mysql://localhost:3306/test?useSSL=true&serverTimezone=UTC', " +
      " 'connector.table' = 'demo', " +
      " 'connector.driver' = 'com.mysql.cj.jdbc.Driver', " +
      " 'connector.username' = 'root', " +
      " 'connector.password' = '123456'" +
      ")"
  }

  // kafka-topics --create --zookeeper skuldcdhtest1.ktcs.com:2181 --replication-factor 1 --partitions 1 --topic test01
  // kafka-console-consumer --bootstrap-server skuldcdhtest1.ktcs.com:9092 --topic test01 --from-beginning
  def createKafkaTable(): String = {
    """
      |CREATE TABLE demo1 (
      |    uid VARCHAR COMMENT 'uid',
      |    rid VARCHAR COMMENT 'rid'
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