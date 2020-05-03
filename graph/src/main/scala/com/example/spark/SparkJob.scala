package com.example.spark

import com.example.job.Job

/**
  * @author XiaShuai on 2020/4/24.
  */
trait SparkJob extends Job {
  /**
    * 子类实现具体计算操作
    */
  override def process(): Unit = {
    execute()
  }

  def execute(): Unit
}
