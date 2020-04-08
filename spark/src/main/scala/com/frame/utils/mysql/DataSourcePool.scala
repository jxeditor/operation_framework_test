package com.frame.utils.mysql

import java.util.concurrent.ConcurrentHashMap

import com.frame.log.Logging
import com.mchange.v2.c3p0.ComboPooledDataSource
import javax.sql.DataSource

/**
  * 获得c3p0连接池对象
  * 单例
  * 线程安全
  * @author XiaShuai on 2020/4/8.
  */
object DataSourcePool extends Logging {
  /**
    * key = jdbcUrl
    * value = data source
    */
  private lazy val pool = new ConcurrentHashMap[String, ComboPooledDataSource]

  def getDataSource(jdbcUrl: String): DataSource = {
    pool.computeIfAbsent(jdbcUrl, new java.util.function.Function[String, ComboPooledDataSource]() {
      override def apply(t: String): ComboPooledDataSource = {
        val source = new ComboPooledDataSource
        source.setJdbcUrl(jdbcUrl)
        source.setDriverClass("com.mysql.cj.jdbc.Driver") // version=8.0.15 com.mysql.cj.jdbc.Driver 根据实际依赖包设置
        source.setMaxPoolSize(3) // 默认 15
        logWarn("ComboPooledDataSource does not exist {} , init success", source.getJdbcUrl)
        sys.addShutdownHook(source.close())
        source
      }
    })
  }
}
