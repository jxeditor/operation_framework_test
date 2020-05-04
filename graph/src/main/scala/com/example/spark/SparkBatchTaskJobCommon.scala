package com.example.spark

import com.example.job.Task
import com.example.spark.PredefSparkJob.Session
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext

import scala.collection.mutable

/**
  * @author XiaShuai on 2020/4/24.
  */
class SparkBatchTaskJobCommon(implicit val session: Session) extends SparkTaskJob[BatchTaskCommon] {
  override def taskArray(): Array[BatchTaskCommon] = {
    // TODO 根据app,dt生成Task数组
    val tasks = executeTasks()
    var taskInstance = mutable.ArrayBuffer.empty[BatchTaskCommon]
    for (task <- tasks) {
      taskInstance.+=(task.getConstructor(classOf[SparkSession]).newInstance(session))
    }
    taskInstance.toArray
  }

  def executeTasks(): Array[Class[_ <: BatchTaskCommon]] = Array()
}


class BatchTaskCommon(implicit val session: Session) extends Task {
  override def taskId: String = this.getClass.getSimpleName

  override def process(): Unit = {}
}