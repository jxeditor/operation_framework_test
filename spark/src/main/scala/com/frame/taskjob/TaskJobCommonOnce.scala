package com.frame.taskjob

import com.frame.core.Task
import com.frame.gamejob.GameCommonJob
import com.frame.job.PredefSparkJob.Session
import com.frame.job.inter.SparkTaskJob
import com.frame.tools.conf.Conf

import scala.collection.mutable

/**
  * @author XiaShuai on 2020/4/8.
  */
class TaskJobCommonOnce(implicit override val conf: Conf)
  extends SparkTaskJob[TaskOnce] with GameCommonJob {
  /** 待执行 Task 列表 */
  override def taskArray(): Array[TaskOnce] = {
    val exeTask = executeTasks()
    var taskInstance = mutable.ArrayBuffer.empty[TaskOnce]
    // 根据需求循环添加Task数组
    taskInstance.toArray
  }

  /** TaskJob 运行 tasks class */
  def executeTasks(): Array[Class[_ <: TaskGame]] = Array()
}


/** 仅执行一次 task */
class TaskOnce(implicit val session: Session, val jcs: JobOnceContext) extends Task {

  override def taskId: String = s"${jcs.appId}.${super.taskId}"

  override def execute(): Unit = {}
}

case class JobOnceContext(conf: Conf, appId: String) {}
