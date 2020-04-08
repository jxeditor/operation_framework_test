package com.frame.gamejob

import com.frame.job.SparkJob

/**
  * 游戏任务整体抽象实现,主要是通过外部存储获取业务信息
  * @author XiaShuai on 2020/4/8.
  */
trait GameJob extends SparkJob {

}

/**
  * 基础任务
  */
trait GameCommonJob extends GameJob {
  override def jobType: String = "common"
}

/**
  * 小时任务
  */
trait GameHoursJob extends GameCommonJob {
  override def jobType: String = "hours"
}

/**
  * 一次任务
  */
trait GameCommonOnceJob extends GameCommonJob {
  override def jobType: String = "clean"
}

/** 实时任务 */
trait GameStreamingJob extends GameJob {
  override def jobType: String = "streaming"
}

/**
  * 任务运行以来数据
  */
class JobData()
  extends Serializable {

}