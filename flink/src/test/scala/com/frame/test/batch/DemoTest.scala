package com.frame.test.batch

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.GlobalConfiguration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.config.{ExecutionConfigOptions, OptimizerConfigOptions}
import org.apache.flink.table.api.java.StreamTableEnvironment
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, SqlDialect, TableEnvironment, TableUtils}
import org.apache.flink.table.catalog.hive.HiveCatalog

/**
  * @author XiaShuai on 2020/5/20.
  */
object DemoTest {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val btEnv = BatchTableEnvironment.create(env)
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build()
    val tEnv = TableEnvironment.create(settings)
    val env1 = StreamExecutionEnvironment.getExecutionEnvironment
    val settings2 = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build
    val stEnv = StreamTableEnvironment.create(env1, settings2)

    tEnv.getConfig.getConfiguration.setBoolean(OptimizerConfigOptions.TABLE_OPTIMIZER_JOIN_REORDER_ENABLED, true)
    tEnv.getConfig.getConfiguration.setBoolean(OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SOURCE_ENABLED, false)
    tEnv.getConfig.getConfiguration.setBoolean(OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SUB_PLAN_ENABLED, false)
    tEnv.getConfig.getConfiguration.setBoolean(OptimizerConfigOptions.TABLE_OPTIMIZER_SOURCE_PREDICATE_PUSHDOWN_ENABLED, false)
    tEnv.getConfig.getConfiguration.setLong(OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD, 10485760L)
    tEnv.getConfig.getConfiguration.setInteger(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1)
    tEnv.getConfig.getConfiguration.setInteger(ExecutionConfigOptions.TABLE_EXEC_SORT_DEFAULT_LIMIT, 200)
    tEnv.getConfig.addConfiguration(GlobalConfiguration.loadConfiguration)

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
    tEnv.registerFunction("udfOnlineTime", new OnlineTimeUdf)

    val table = tEnv.sqlQuery(
      s"""
         |SELECT rid, sum(cast(p['t_t4_d'] as int)) as pay
         |FROM  game_ods.event
         |WHERE app   = 'game_skuld_01'
         |  AND dt    = '2019-08-16'
         |  AND event = 'event_role.pay_3_d'
         |GROUP by rid
         |""".stripMargin).select("rid")
    //    val rows = TableUtils.collectToList(table)
    //    rows.toArray().foreach(println)


    tEnv.insertInto("`default`.test", table)
    tEnv.execute("")
  }
}
