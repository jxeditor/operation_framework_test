package com.frame.job.inter

import com.frame.core.exception.JobException
import com.frame.core.{Task, TaskJob}
import com.frame.job.SparkJob
import com.frame.core.Job

/**
  * Spark Task Job 类型任务抽象类
  * Job 负责管理 Task 任务运行依赖参数 [[Job]]
  * Job 与 task 关系为 1对多
  *
  * 默认顺序启动所有 task 任务
  * @author XiaShuai on 2020/4/8.
  */
trait SparkTaskJob[T <: Task] extends SparkJob with TaskJob[T] {
  /** 默认实现使用 foreachTask 方法 */
  @throws[JobException]
  override def jobCompute(): Unit = this.foreachTask()
}
