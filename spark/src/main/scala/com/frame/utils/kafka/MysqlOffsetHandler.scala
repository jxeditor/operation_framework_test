package com.frame.utils.kafka

import java.sql.{ResultSet, SQLException}

import com.frame.constant.AnalystSparkConfConstant._
import com.frame.tools.internal.Loggable
import com.frame.tools.lang.Nullable
import com.frame.utils.mysql.DataSourcePool
import com.frame.utils.kafka.MysqlOffsetHandler.{JDBC, REPLACE_INTO_SQL}
import org.apache.commons.dbutils.{QueryRunner, ResultSetHandler}

/**
  * kafka 消费偏移量管理
  * @author XiaShuai on 2020/4/8.
  */
object MysqlOffsetHandler {
  /*
  CREATE DATABASE kafka_consumer DEFAULT CHARACTER SET utf8 DEFAULT COLLATE utf8_general_ci;
  GRANT ALL ON kafka_consumer.* TO 'kafka_consumer'@'%' IDENTIFIED BY '42GRV2ZIxL$pR#zA';
    MYSQL-DDL 建表语句:
create table if not exists consumer_offset
(
    group_id        char(255)                              not null,
    topic           char(255)                              not null,
    topic_partition int                                    not null,
    from_offset     mediumtext                             not null,
    until_offset    mediumtext                             not null,
    count           mediumtext                             not null,
    last_modified   datetime default '1970-01-01 00:00:00' null,
    primary key (group_id, topic, topic_partition)
)
    comment 'kafka 消费偏移量管理';
   */
  private val JDBC: String = getConf.getString(SK_MYSQL_KAFKA_CONSUMER_JDBC_URL)
  private val REPLACE_INTO_SQL: String = "REPLACE INTO consumer_offset" +
    "(group_id, topic, topic_partition, from_offset, until_offset, count, last_modified)" +
    "VALUES (?, ?, ?, ?, ?, ?, ?)"
}

class MysqlOffsetHandler extends OffsetHandler with Loggable {

  @Nullable
  @throws[ConsumerOffsetHandlerException]
  override def fetch(groupId: String, topicsSet: Set[String]): Seq[ConsumerOffsetBean] = {
    val qr: QueryRunner = new QueryRunner(DataSourcePool.getDataSource(JDBC))
    try {
      val sql = String.format("SELECT * FROM consumer_offset WHERE group_id=? AND topic in(%s)",
        s"'${topicsSet.mkString("','")}'")
      logWarn(s"fetch groupId:${} , topicsSet:$topicsSet ,sql:$sql")
      qr.query(sql, new KafkaResultSetHandler, groupId)
    } catch {
      case e: SQLException =>
        logError("fetch error {}", e.getMessage)
        throw new ConsumerOffsetHandlerException("fetch error", e)
    }
  }

  /**
    * 以上代码可能出现异常 "Result type of an implicit conversion must be more specific than AnyRef"
    * 默认全部 .toString 处理
    */
  @Nullable
  @throws[ConsumerOffsetHandlerException]
  override def replaceInto(beans: Seq[ConsumerOffsetBean]): Array[Int] = {
    // 二维数组定义 (一维长度、二维长度)
    val params: Array[Array[Object]] = Array.ofDim(beans.size, 7)
    for (i <- beans.indices) {
      val bean: ConsumerOffsetBean = beans(i)
      params(i)(0) = bean.groupId.toString
      params(i)(1) = bean.topicPar.topic.toString
      params(i)(2) = bean.topicPar.partition.toString
      params(i)(3) = bean.fromOffset.toString
      params(i)(4) = bean.untilOffset.toString
      params(i)(5) = bean.count().toString
      params(i)(6) = bean.lastModified.toString
    }
    val qr: QueryRunner = new QueryRunner(DataSourcePool.getDataSource(JDBC))
    // 注意，给sql语句设置参数的时候，按照user表中字段的顺序
    try {
      qr.batch(REPLACE_INTO_SQL, params)
    } catch {
      case e: SQLException =>
        logError("batch error {}", e.getMessage)
        throw new ConsumerOffsetHandlerException("replaceInto error", e)
    }
  }
}

/** 使用 org.apache.commons.dbutils.ResultSetHandler 对查询结果转换为对象信息实现 */
private class KafkaResultSetHandler extends ResultSetHandler[Seq[ConsumerOffsetBean]] {
  @throws[SQLException]
  override def handle(rs: ResultSet): Seq[ConsumerOffsetBean] = {
    val r = Seq.newBuilder[ConsumerOffsetBean]
    while (rs.next) {
      r.+=(ConsumerOffsetBean(
        rs.getString("group_id"),
        TopicPar(rs.getString("topic"), rs.getInt("topic_partition")),
        rs.getLong("from_offset"),
        rs.getLong("until_offset")
      ))
    }
    r.result()
  }
}

