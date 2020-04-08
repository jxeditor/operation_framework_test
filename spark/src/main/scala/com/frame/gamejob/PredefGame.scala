package com.frame.gamejob

import com.frame.taskjob.TaskGameContext
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.InputDStream
import org.json.JSONObject

/**
  * 任务相关
  * @author XiaShuai on 2020/4/8.
  */
object PredefGame {
  type TGC = TaskGameContext
  type RecordOriginalEvent = ConsumerRecord[String, JSONObject]
  type DStreamOriginalEvent = InputDStream[RecordOriginalEvent]
}
