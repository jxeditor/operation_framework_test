package com.example.core

import java.util

/**
  * @author XiaShuai on 2020/5/4.
  */
object FlinkGraph {

  class Builder {
    private[this] val topics = new util.ArrayList[String]()

    def inBatchMode(): Builder = synchronized {
      this
    }

    def inStreamMode(): Builder = synchronized {
      this
    }

    def topics(): util.ArrayList[String] = new util.ArrayList[String]()

//    def draw(): Graph = {
//
//    }
  }

  def newInstance(): Builder = new Builder
}
