package com.example.core

/**
  * @author XiaShuai on 2020/5/3.
  */
trait Graph {
  def addPoint(name: String, point: Point)
  def addEdge(from: String, to: String, filter: AnyRef => Boolean)
  def finish()
}
