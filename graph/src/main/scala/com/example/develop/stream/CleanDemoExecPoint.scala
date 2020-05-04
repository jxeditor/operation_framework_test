package com.example.develop.stream

import com.example.core.ExecPoint
import com.example.job.Job
import org.apache.spark.streaming.StreamingContext

/**
  * @author XiaShuai on 2020/5/3.
  */
class CleanDemoExecPoint(conf: String) extends ExecPoint[StreamingContext] {
  override def process(sc: StreamingContext): Unit = {
    val job = Class.forName(conf).getConstructor(classOf[StreamingContext]).newInstance(sc).asInstanceOf[Job]
    println(job.getClass.getSimpleName)
    job.process()
  }
}
