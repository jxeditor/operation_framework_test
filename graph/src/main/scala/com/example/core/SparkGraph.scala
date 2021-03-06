package com.example.core

import java.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author XiaShuai on 2020/4/23.
  */
object SparkGraph {

  class Builder {
    private[this] val conf = new SparkConf()
    private[this] val topics = Array[String]()

    def inBatchMode(): Builder = synchronized {
      conf.setMaster("local").setAppName("batch").set("executor-mode", "batch")
      this
    }

    def inStreamMode(): Builder = synchronized {
      conf.setMaster("local[*]").setAppName("stream").set("executor-mode", "stream")
      this
    }

    def kafkaOption(): Builder = synchronized {
      conf.set("spark.streaming.kafka.maxRatePerPartition", "10")
        .set("spark.streaming.kafka.consumer.poll.ms", "120000")
        .set("spark.streaming.backpressure.enabled", "true") // 开启kafka背压机制
      this
    }

    def draw(): Graph = {
      val session = SparkSession.builder.config(conf).enableHiveSupport.getOrCreate
      if (conf.get("executor-mode").equals("stream")) {
        val sc: StreamingContext = new StreamingContext(session.sparkContext, Seconds(5))
        new SparkStreamGraph(sc)
      } else {
        new SparkBatchGraph(session)
      }
    }
  }

  def newInstance(): Builder = new Builder
}

class SparkBatchGraph(session: SparkSession) extends GraphImpl {
  /** 连接节点 */
  override def linkPoint(name: String, point: Point): Unit = {
    point match {
      case value: ExecPoint[SparkSession] =>
        value.process(session)
        val toMap = edgeMap(name)
        toMap.foreach(x => {
          val toName = x._1
          val toPoint = pointMap(toName)
          linkPoint(toName, toPoint)
        })
      case _ =>
        point.asInstanceOf[EndPoint[SparkSession]].process(session)
    }
  }
}

class SparkStreamGraph(sc: StreamingContext) extends GraphImpl {
  /** 连接节点 */
  override def linkPoint(name: String, point: Point): Unit = {
    point match {
      case value: ExecPoint[StreamingContext] =>
        value.process(sc)
        val cost1 = System.currentTimeMillis()
        val toMap = edgeMap(name)
        toMap.foreach(x => {
          val toName = x._1
          val toPoint = pointMap(toName)
          linkPoint(toName, toPoint)
        })
      case _ =>
        point.asInstanceOf[EndPoint[StreamingContext]].process(sc)
    }
  }
}

