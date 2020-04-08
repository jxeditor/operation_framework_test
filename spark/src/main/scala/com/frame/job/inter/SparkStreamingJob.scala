package com.frame.job.inter

import com.frame.constant.AnalystSparkConfConstant.SK_SPARK_STREAMING_DURATION_SECONDS
import com.frame.core.exception.JobException
import com.frame.job.SparkJob
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
  * spark streaming job 抽象实现.
  * @author XiaShuai on 2020/4/8.
  */
trait SparkStreamingJob extends SparkJob {
  private val _sparkBatchDuration = conf.getInt(SK_SPARK_STREAMING_DURATION_SECONDS)
  private val _sStreamingContext: StreamingContext =
    new StreamingContext(sparkContext, Duration.apply(_sparkBatchDuration * 1000L))

  @throws[JobException]
  override final def jobCompute(): Unit = {

    this.streamingJobCompute()
    streamingContext.start()
    logWarn(s"streamingJobCompute StreamingContext#start duration [${_sparkBatchDuration.toString} second]...")
    streamingContext.awaitTermination()
    logWarn("streamingJobCompute StreamingContext#awaitTermination ...")
  }

  def streamingContext: StreamingContext = _sStreamingContext

  /**
    * 流式任务计算实现.
    * @throws JobException the job exception
    */
  @throws[JobException]
  def streamingJobCompute(): Unit

  override def close(): Unit = {
    super.close()
    if (this._sStreamingContext != null) try
      this._sStreamingContext.stop(true)
    catch {
      case _: Exception =>
    }
  }
}
