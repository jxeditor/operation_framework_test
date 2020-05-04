package com.example.jobtasks

import com.example.spark.PredefSparkJob.Session
import com.example.spark.BatchTaskCommon

/**
  * @author XiaShuai on 2020/4/24.
  */
class Test1Task(implicit override val session: Session) extends BatchTaskCommon {
  override def process(): Unit = {
    session.sql("select * from test").show(10)
    println("task1")
  }
}
