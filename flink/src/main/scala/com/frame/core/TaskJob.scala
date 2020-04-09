package com.frame.core

import com.frame.core.TaskJob.logLineLength
import com.frame.core.exception.{TaskException, TaskJobException}
import com.frame.tools.string.StringUtil.headTailPadding
import com.frame.tools.time.TimesTools

/**
  * @author XiaShuai on 2020/4/8.
  */
trait TaskJob[T <: Task] extends Job {

  /** 待执行 Task 列表 */
  def taskArray(): Array[T]

  /** 循环执行 task 任务 */
  @throws[TaskJobException]
  def foreachTask(): Unit = {
    val ts = this.taskArray()
    val length = ts.length

    val runLog = Seq.newBuilder[String] // task 执行日志记录
    var exeSec = 0L // 执行总耗时 s
    logWarn(headTailPadding(s" job foreachTask[$length] start ", "-", logLineLength))
    for (i <- ts.indices) {
      val t = ts(i)
      val taskLogKey = s"< exe [${"%4d".format(i + 1)} / $length ] task [${t.taskId}]  >"
      logWarn(headTailPadding(taskLogKey, "-", logLineLength))
      val startTime = TimesTools.nowUnixSec()
      try {
        t.execute()
        val timeConsumingSec = TimesTools.nowUnixSec() - startTime // 耗时 s
        exeSec += timeConsumingSec
        runLog.+=("%-75s .........SUCCESS [%5d s]".format(taskLogKey, timeConsumingSec))
      } catch {
        case e: TaskException =>
          runLog.+=("%-75s ......ERROR [%5d s]".format(taskLogKey, (TimesTools.nowUnixSec() - startTime)))
          throw new TaskJobException(t.getClass.getName + " execute error ", e)
      }
    }
    logWarn(headTailPadding(s" job foreachTask[$length] end ", "-", logLineLength))
    logWarn(s"Reactor Summary $id: SUCCESS [$exeSec s]")
    runLog.result().foreach(logWarn)
  }
}

object TaskJob {
  val logLineLength = 110 // log 日志长度限制
}

