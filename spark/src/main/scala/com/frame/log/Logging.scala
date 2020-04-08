package com.frame.log

import com.frame.tools.internal.Loggable
import org.slf4j.{Logger, LoggerFactory}


/**
  * @author XiaShuai on 2020/4/8.
  */
trait Logging extends Loggable{

  @transient private var log_ : Logger = _

  override def logger(): Logger = log

  def log: Logger = {
    if (log_ == null) {
      log_ = LoggerFactory.getLogger(logName)
    }
    log_
  }

  def logName: String = {
    this.getClass.getName.stripSuffix("$")
  }
}
