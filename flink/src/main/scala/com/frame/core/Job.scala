package com.frame.core

import com.frame.core.exception.JobException
import com.frame.log.Logging

/**
  * @author XiaShuai on 2020/4/8.
  */
trait Job extends Logging with AutoCloseable{
  /** Job 唯一 ID */
  def id: String = this.getClass.getSimpleName

  /** 执行任务 */
  @throws[JobException]
  def execute(): Unit

  /** 资源关闭 */
  @throws[Exception]
  override def close(): Unit = {
    // 默认不进行处理
  }
}
