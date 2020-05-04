package com.example.develop.batch

import com.example.core.ExecPoint
import com.example.job.Job
import com.example.spark.PredefSparkJob.Session
import org.apache.spark.sql.SparkSession

/**
  * @author XiaShuai on 2020/4/23.
  */
class DwaDemoExecPoint(info: String) extends ExecPoint[Session] {
  override def process(session: Session): Unit = {
    // TODO 取dwa层级任务,进行循环执行
    val job = Class.forName("com.example.jobtasks.Test1Job").getConstructor(classOf[SparkSession]).newInstance(session).asInstanceOf[Job]
    println(job.getClass.getSimpleName)
    job.process()
  }
}
