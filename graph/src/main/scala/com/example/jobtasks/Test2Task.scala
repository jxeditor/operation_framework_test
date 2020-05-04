package com.example.jobtasks

import com.example.spark.StreamTaskCommon
import org.apache.spark.streaming.StreamingContext

/**
  * @author XiaShuai on 2020/4/24.
  */
class Test2Task(implicit override val sc: StreamingContext) extends StreamTaskCommon {
  override def process(): Unit = {
    println("task2")
    val ds = sc.socketTextStream("skuldcdhtest1.ktcs", 10086)

    val result = ds.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)

    result.print()
  }
}
