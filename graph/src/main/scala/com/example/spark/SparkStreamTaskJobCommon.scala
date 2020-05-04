package com.example.spark

import com.example.job.Task
import org.apache.spark.streaming.StreamingContext

import scala.collection.mutable

/**
  * @author XiaShuai on 2020/5/4.
  */
class SparkStreamTaskJobCommon(implicit val sc: StreamingContext) extends SparkTaskJob[StreamTaskCommon] {
  override def taskArray(): Array[StreamTaskCommon] = {
    // TODO 根据app,dt生成Task数组
    val tasks = executeTasks()
    var taskInstance = mutable.ArrayBuffer.empty[StreamTaskCommon]
    for (task <- tasks) {
      taskInstance.+=(task.getConstructor(classOf[StreamingContext]).newInstance(sc))
    }
    taskInstance.toArray
  }

  def executeTasks(): Array[Class[_ <: StreamTaskCommon]] = Array()
}


class StreamTaskCommon(implicit val sc: StreamingContext) extends Task {
  override def taskId: String = this.getClass.getSimpleName

  override def process(): Unit = {}
}