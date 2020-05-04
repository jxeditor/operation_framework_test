package com.example.develop.batch

import com.example.core.EndPoint
import com.example.spark.PredefSparkJob.Session

/**
  * @author XiaShuai on 2020/4/23.
  */
class AdsDemoEndPoint extends EndPoint[Session] {
  override def process(session: Session): Unit = {
    println("ads" + session.hashCode())
  }
}
