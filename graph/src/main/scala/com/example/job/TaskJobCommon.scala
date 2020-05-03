package com.example.job

import com.example.spark.PredefSparkJob.Session
import com.example.spark.SparkTaskJob
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
  * @author XiaShuai on 2020/4/24.
  */
class TaskJobCommon(implicit val session: Session) extends SparkTaskJob[TaskCommon] {
  override def taskArray(): Array[TaskCommon] = {
    // TODO 根据app,dt生成Task数组
    val tasks = executeTasks()
    var taskInstance = mutable.ArrayBuffer.empty[TaskCommon]
    for (task <- tasks) {
      taskInstance.+=(task.getConstructor(classOf[SparkSession]).newInstance(session))
    }
    taskInstance.toArray
  }

  def executeTasks(): Array[Class[_ <: TaskCommon]] = Array()
}


class TaskCommon(implicit val session: Session) extends Task {
  override def taskId: String = this.getClass.getSimpleName

  override def process(): Unit = {}
}