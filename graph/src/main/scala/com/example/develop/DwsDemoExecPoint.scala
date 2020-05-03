package com.example.develop

import com.example.spark.PredefSparkJob.Session
import com.example.core.ExecPoint

/**
  * @author XiaShuai on 2020/4/23.
  */
class DwsDemoExecPoint extends ExecPoint[Session] {
  override def process(session: Session): Unit = {
    // 获取Task列表
    println("dws" + session.hashCode())
  }
}
