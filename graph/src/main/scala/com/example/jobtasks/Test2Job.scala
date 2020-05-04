package com.example.jobtasks

import com.example.spark.{SparkStreamTaskJobCommon, StreamTaskCommon}
import org.apache.spark.streaming.StreamingContext

/**
  * @author XiaShuai on 2020/4/24.
  */
class Test2Job(implicit override val sc: StreamingContext) extends SparkStreamTaskJobCommon {
  override def executeTasks(): Array[Class[_ <: StreamTaskCommon]] = {
    Array(classOf[Test2Task])
  }
}
