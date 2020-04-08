package com.frame.taskjob

import java.time.LocalDate

import com.frame.core.Task
import com.frame.gamejob.GameCommonOnceJob
import com.frame.job.PredefSparkJob.Session
import com.frame.job.inter.SparkTaskJob
import com.frame.tools.conf.Conf

import scala.collection.mutable

/**
  * @author XiaShuai on 2020/4/8.
  */
class TaskJobCommonDay(implicit override val conf: Conf)
  extends SparkTaskJob[TaskDay] with GameCommonOnceJob {
  /** 待执行 Task 列表 */
  override def taskArray(): Array[TaskDay] = {
    val exeTask = executeTasks()
    var taskInstance = mutable.ArrayBuffer.empty[TaskDay]
    // 根据需求循环添加Task数组
    taskInstance.toArray
  }

  /** TaskJob 运行 tasks class */
  def executeTasks(): Array[Class[_ <: TaskGame]] = Array()
}


/** 仅执行一次 task */
class TaskDay(implicit val session: Session, val jdc: JobDayContext) extends Task {

  override def taskId: String = s"$jdc.${super.taskId}"

  override def execute(): Unit = {}
}

case class JobDayContext() {}