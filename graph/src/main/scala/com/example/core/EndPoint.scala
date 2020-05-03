package com.example.core

/**
  * @author XiaShuai on 2020/4/23.
  */
trait EndPoint[T] extends Point {
  def process(V: T)
}
