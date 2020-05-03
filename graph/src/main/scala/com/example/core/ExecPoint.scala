package com.example.core

import com.example.spark.PredefSparkJob.Session

/**
  * @author XiaShuai on 2020/4/23.
  */
trait ExecPoint[T] extends Point {
  def process(V: T)
}
