package com.frame.job

import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.catalog.hive.HiveCatalog
import org.apache.flink.table.descriptors.{FileSystem, Json, Schema}
import org.apache.flink.table.types.DataType
import org.apache.flink.types.Row
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._

/**
  * @author XiaShuai on 2020/4/8.
  */
object Hive {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build
    val stEnv = StreamTableEnvironment.create(env, bsSettings)

    val hiveCatalog = new HiveCatalog("test", "default",
      "F:\\operation_framework_test\\flink\\src\\main\\resources\\hive_conf", "2.1.1")

    stEnv.registerCatalog("test", hiveCatalog)
    stEnv.useCatalog("test")
    val sql =
      s"""
         |select p
         |from game_ods.event
         |where app='game_skuld_01'
         |and dt = '2019-09-25'
         |and event='event_user.track_2'
         |limit 1
         |""".stripMargin

    val src = stEnv.sqlQuery(sql)
    src.toAppendStream[Row].print().setParallelism(1)
    stEnv.execute("test")
  }
}
