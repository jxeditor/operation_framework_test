package com.example.develop

import com.example.core.ExecPoint
import com.example.job.Job
import com.example.spark.PredefSparkJob.Session
import org.apache.spark.sql.SparkSession

/**
  * @author XiaShuai on 2020/5/3.
  */
class CleanDemoExecPoint extends ExecPoint[Session] {
  override def process(session: Session): Unit = {
    val job = Class.forName("com.example.jobtasks.Test1Job").getConstructor(classOf[SparkSession]).newInstance(session).asInstanceOf[Job]
    println(job.getClass.getSimpleName)
    job.process()
  }
}
