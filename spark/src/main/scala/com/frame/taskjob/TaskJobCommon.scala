package com.frame.taskjob

import java.time.LocalDate

import com.frame.core.Task
import com.frame.gamejob.PredefGame.TGC
import com.frame.gamejob.{GameCommonJob, GameJobSetting}
import com.frame.job.PredefSparkJob.Session
import com.frame.job.inter.SparkTaskJob
import com.frame.tools.conf.Conf

import scala.collection.mutable

/**
  * @author XiaShuai on 2020/4/8.
  */
class TaskJobCommon(implicit override val conf: Conf)
  extends SparkTaskJob[TaskGame] with GameCommonJob {
  /** 待执行 Task 列表 */
  override def taskArray(): Array[TaskGame] = {
    val exeTask = executeTasks()
    var taskInstance = mutable.ArrayBuffer.empty[TaskGame]
    // 根据需求循环添加Task数组
    taskInstance.toArray
  }

  /** TaskJob 运行 tasks class */
  def executeTasks(): Array[Class[_ <: TaskGame]] = Array()

  /** TaskJob 运行 时间跨度，比如数据粒度为天、周、月 */
  def dtPlus(dt: LocalDate): LocalDate = dt.plusDays(1)
}

/** 按 TaskGameContext 运行 task */
class TaskGame(implicit val session: Session, val tgc: TGC) extends Task {

  override def taskId: String = s"游戏ID.$executeDt.${super.taskId}"

  override def execute(): Unit = {}

  /** task 执行任务实际日期 */
  def executeDt: String = "日期"
}

case class TaskGameContext() {}