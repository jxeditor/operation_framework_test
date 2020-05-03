package com.example.spark

import com.example.job.{Task, TaskJob}

/**
  * @author XiaShuai on 2020/4/24.
  */
trait SparkTaskJob[T <: Task] extends SparkJob with TaskJob[T] {
  override def execute(): Unit = this.foreachTask()
}
