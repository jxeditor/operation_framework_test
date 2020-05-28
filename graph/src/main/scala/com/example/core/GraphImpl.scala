package com.example.core

import scala.collection.mutable

/**
  * @author XiaShuai on 2020/5/3.
  */
abstract class GraphImpl[T](V: T) extends Graph {
  val pointMap: mutable.Map[String, Point] = mutable.Map[String, Point]()
  val edgeMap: mutable.Map[String, mutable.Map[String, AnyRef => Boolean]] = mutable.Map[String, mutable.Map[String, AnyRef => Boolean]]()

  def addPoint(name: String, point: Point): Unit = {
    pointMap(name) = point
  }

  def addEdge(from: String, to: String, filter: AnyRef => Boolean): Unit = {
    val filterMap = edgeMap.get(from)
    if (filterMap.isDefined) {
      val map = filterMap.get
      map += (to -> filter)
    }
    else {
      edgeMap += (from -> mutable.Map(to -> filter))
    }
  }

  def finish(): Unit = {
    val outSize = edgeMap.map(x => (x._1, x._2.size))
    val inSize = edgeMap.map(x => x._2.toList).reduce((a, b) => a ::: b).groupBy(x => x._1).map(x => (x._1, x._2.size))
    val rootPoint = pointMap.filter(x => outSize.getOrElse(x._1, 0) != 0 && inSize.getOrElse(x._1, 0) == 0).toList

    rootPoint.foreach(x => linkPoint(x._1, x._2))
  }

  /** 连接节点 */
  def linkPoint(name: String, point: Point)
}
