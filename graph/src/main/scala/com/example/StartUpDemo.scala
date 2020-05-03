package com.example

import com.example.develop.{AdsDemoEndPoint, CleanDemoExecPoint, DwaDemoExecPoint, DwsDemoExecPoint}
import com.example.core.{SparkBatchGraph, SparkGraph}

/**
  * @author XiaShuai on 2020/4/23.
  */
object StartUpDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "E:\\Soft\\hadoop-2.8.0")
        val graph = SparkGraph.newInstance().inBatchMode().draw()
        graph.addPoint("dwa", new DwaDemoExecPoint("game1,game2"))
        graph.addPoint("dws", new DwsDemoExecPoint)
        graph.addPoint("ads", new AdsDemoEndPoint)

        graph.addEdge("dwa","dws", null)
        graph.addEdge("dws","ads", null)
        graph.finish()

//    val graph = SparkBatchGraph.newInstance().inStreamMode().draw()
//    graph.addPoint("clean", new CleanDemoExecPoint())
//    graph.addPoint("ads", new AdsDemoEndPoint)
//
//    graph.addEdge("clean","ads", null)
//    graph.finish()
  }
}
