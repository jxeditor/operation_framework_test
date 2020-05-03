package com.example.job

/**
  * @author XiaShuai on 2020/4/24.
  */
trait Task {
  def taskId: String = this.getClass.getSimpleName

  def process()
}
