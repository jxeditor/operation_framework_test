package com.example.jobtasks

import com.example.spark.PredefSparkJob.Session
import com.example.spark.{BatchTaskCommon, SparkBatchTaskJobCommon}

/**
  * @author XiaShuai on 2020/4/24.
  */
class Test1Job(implicit override val session: Session) extends SparkBatchTaskJobCommon {
  override def executeTasks(): Array[Class[_ <: BatchTaskCommon]] = {
    Array(classOf[Test1Task])
  }
}
