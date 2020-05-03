package com.example.jobtasks

import com.example.job.{TaskCommon, TaskJobCommon}
import com.example.spark.PredefSparkJob.Session

/**
  * @author XiaShuai on 2020/4/24.
  */
class Test2Job(implicit override val session: Session) extends TaskJobCommon {
  override def executeTasks(): Array[Class[_ <: TaskCommon]] = {
    Array(classOf[Test2Task])
  }
}
