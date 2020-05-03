package com.example.job

/**
  * @author XiaShuai on 2020/4/24.
  */
trait TaskJob[T <: Task] extends Job {
  def taskArray(): Array[T]

  def foreachTask(): Unit = {
    val tasks = this.taskArray()
    val len = tasks.length
    for (i <- tasks.indices) {
      val task = tasks(i)
      task.process()
    }
  }
}
