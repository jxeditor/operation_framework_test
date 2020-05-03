package com.example.jobtasks

import com.example.job.TaskCommon
import com.example.spark.PredefSparkJob.Session

/**
  * @author XiaShuai on 2020/4/24.
  */
class Test1Task(implicit override val session: Session) extends TaskCommon {
  override def process(): Unit = {
    session.sql("select * from test").show(10)
    println("task1")
  }
}
