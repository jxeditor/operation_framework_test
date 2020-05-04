package com.example

import com.example.core.SparkGraph
import com.example.develop.stream.{CleanDemoExecPoint, StreamEndPoint}

/**
  * @author XiaShuai on 2020/4/23.
  */
object StartUpDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "E:\\Soft\\hadoop-2.8.0")
    //        val graph = SparkGraph.newInstance().inBatchMode().draw()
    //        graph.addPoint("dwa", new DwaDemoExecPoint("game1,game2"))
    //        graph.addPoint("dws", new DwsDemoExecPoint)
    //        graph.addPoint("ads", new AdsDemoEndPoint)
    //
    //        graph.addEdge("dwa","dws", null)
    //        graph.addEdge("dws","ads", null)
    //        graph.finish()

    val graph = SparkGraph.newInstance().inStreamMode().draw()
    graph.addPoint("clean", new CleanDemoExecPoint("com.example.jobtasks.Test2Job"))
    graph.addPoint("end", new StreamEndPoint())

    graph.addEdge("clean", "end", null)
    graph.finish()
  }
}
