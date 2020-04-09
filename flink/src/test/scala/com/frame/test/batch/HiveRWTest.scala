package com.frame.test.batch

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.hcatalog.scala.HCatInputFormat
import org.junit.{Assert, Test}

import org.apache.flink.api.scala._
import org.apache.flink.types.Row
import org.apache.hadoop.conf.Configuration

/**
  * @author XiaShuai on 2020/4/8.
  */
@Test
class HiveRWTest extends Assert {

  @Test
  def readHiveTest(): Unit = {
    val conf = new Configuration()
    conf.set("hive.metastore.local", "false")

    conf.set("hive.metastore.uris", "thrift://119.3.198.126:9083")
    // 如果是高可用 就需要是nameserver
    // conf.set("hive.metastore.uris", "thrift://172.10.4.142:9083")

    val env = ExecutionEnvironment.getExecutionEnvironment

    //todo 返回类型
    val dataset: DataSet[Row] = env.createInput(new HCatInputFormat[Row]("default", "test", conf))

    dataset.first(10).print()


  }

}
