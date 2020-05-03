package com.example.develop

import com.example.spark.PredefSparkJob.Session
import com.example.core.{EndPoint, ExecPoint}

/**
  * @author XiaShuai on 2020/4/23.
  */
class AdsDemoEndPoint extends EndPoint[Session] {
  override def process(session: Session): Unit = {
    println("ads" + session.hashCode())
  }
}
