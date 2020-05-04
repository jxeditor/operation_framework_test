package com.example.develop.stream

import com.example.core.EndPoint
import org.apache.spark.streaming.StreamingContext

/**
  * @author XiaShuai on 2020/5/4.
  */
class StreamEndPoint extends EndPoint[StreamingContext]{
  override def process(sc: StreamingContext): Unit = {
    // TODO
    println("end")
    sc.start()
    sc.awaitTermination()
  }
}
