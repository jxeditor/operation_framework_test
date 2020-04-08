package com.frame.core

import com.frame.core.exception.TaskException

/**
  * @author XiaShuai on 2020/4/8.
  */
trait Task {
  /** task 唯一 ID */
  def taskId: String = this.getClass.getSimpleName

  @throws[TaskException]
  def execute(): Unit
}
