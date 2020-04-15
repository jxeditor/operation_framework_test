package com.frame.test.batch

import java.util.{Collections, Optional, Properties}
import java.lang.String.format
import java.time.{Instant, LocalDate, LocalTime, ZoneOffset}
import java.util
import java.util.regex.Pattern

import com.alibaba.fastjson.serializer.NameFilter
import com.alibaba.fastjson.{JSON, JSONObject}
import com.google.common.base.Joiner
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer010}
import org.apache.flink.streaming.api.scala._
import org.junit.Test

import scala.collection.mutable
import scala.util.control.Breaks
import scala.util.control.Breaks._

/**
  * @author XiaShuai on 2020/4/10.
  */
@Test
class KafkaRWTest {

  val PARAM_PREFIX = "param_"
  val PARAM_DATA = PARAM_PREFIX + "data"
  val PARAM_EVENT = PARAM_PREFIX + "event"
  val PARAM_ENV = PARAM_PREFIX + "environment"
  val PARAM_USER = PARAM_PREFIX + "user"
  val PARAM_ROLE = PARAM_PREFIX + "role"
  val PARAM_GUILD = PARAM_PREFIX + "guild"

  @Test
  def kafkaSink(): Unit = {
    val READ_TOPIC = "game_log_game_skuld_01"
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val props = new Properties()
    props.put("bootstrap.servers", "skuldcdhtest1.ktcs:9092")
    props.put("group.id", "xs_test")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    val student = env.addSource(new FlinkKafkaConsumer011(
      READ_TOPIC,
      new SimpleStringSchema, props).setStartFromLatest())

    // student.addSink(new FlinkKafkaProducer010("skuldcdhtest1.ktcs:9092", "sink_test", new SimpleStringSchema)).name("sink_test")
    //    val clean = student.map(data => {
    //      val original = new JSONObject(data)
    //      (original.getJSONObject("param_data").toString)
    //    })

    val clean = student.map(new DataCleanMapFunction)

    clean.print()
    env.execute("flink kafka source")
  }

  @Test
  def forTest(): Unit = {
    val map = mutable.HashMap[String, String]()
    val data = JSON.parseObject("{'a':'1','custom':{'c':'2'}}")

    val custom = data.remove("custom")

    custom match {
      case m: util.Map[String, AnyRef] =>
        val iterator = m.entrySet().iterator()
        while (iterator.hasNext) {
          val key = iterator.next().getKey
          map.put(key, m.get(key).toString)
        }
    }
    println(custom.asInstanceOf[util.Map[String, String]].get("c"))
    println(map)
  }
}

class DataCleanMapFunction extends MapFunction[String, EventKafkaGame] {

  val CLEAN_PARAM_DATA = new CleanParamData
  val CLEAN_PARAM_ENV = new CleanParamEvn
  val CLEAN_PARAM_EVENT = new CleanParamEvent
  val CLEAN_PARAM_USER = new CleanParamUser
  val CLEAN_PARAM_ROLE = new CleanParamRole
  val CLEAN_PARAM_GUILD = new CleanParamGuild

  val PARAM_PREFIX = "param_"
  val PARAM_DATA = PARAM_PREFIX + "data"
  val PARAM_EVENT = PARAM_PREFIX + "event"
  val PARAM_ENV = PARAM_PREFIX + "environment"
  val PARAM_USER = PARAM_PREFIX + "user"
  val PARAM_ROLE = PARAM_PREFIX + "role"
  val PARAM_GUILD = PARAM_PREFIX + "guild"
  val DATA_CATEGORY = "category_s"
  val DATA_APP_ID = "app_id_s"
  val DATA_DATA_UNIX = "data_unix"
  val CUSTOM = "custom"
  val DATA_ID = "id_s"

  private val EVENT_PREFIX = "event_"
  val EVENT_APP: String = EVENT_PREFIX + "app"
  val EVENT_ROLE: String = EVENT_PREFIX + "role"
  val EVENT_USER: String = EVENT_PREFIX + "user"
  val EVENT_GUILD: String = EVENT_PREFIX + "guild"

  val PARAM_EVENT_PREFIX = "t_"
  val EVENT_T0_S: String = PARAM_EVENT_PREFIX + "t0_s"
  val DATA_PLATFORM = "platform_s"
  val DATA_REGION = "region_s"
  val DATA_SERVER = "server_s"
  val DATA_CHANNEL = "channel_s"
  val DATA_EVENT_TYPE = "event_type_s"

  val ENV_DEVICE_ID = "device_id_s"
  val ENV_IP = "ip_s"

  val EMPTY_STR = ""

  val PARAM_USER_PREFIX = "u_"
  val EVENT_ORIGINAL_USER_ID = "user_id_s"
  val USER_USER_ID = PARAM_USER_PREFIX + EVENT_ORIGINAL_USER_ID

  val PARAM_ROLE_PREFIX = "r_"
  val PARAM_ROLE_ECO_PREFIX = "r_eco"
  val ROLE_ORIGINAL_ROLE_ID = "role_id_s"
  val EVENT_ORIGINAL_RID = "rid_s"
  val ROLE_RID_S: String = PARAM_ROLE_PREFIX + EVENT_ORIGINAL_RID
  val ROLE_ROLE_ID = PARAM_ROLE_PREFIX + ROLE_ORIGINAL_ROLE_ID

  val DEFAULT_NONE = "none"

  override def map(t: String): EventKafkaGame = {
    val map = mutable.HashMap[String, String]()
    val original = JSON.parseObject(t)

    // 移除非自定义原始数据
    val iterator = original.entrySet().iterator()
    while (iterator.hasNext) {
      if (!iterator.next().getKey.startsWith(PARAM_PREFIX))
        iterator.remove()
    }

    val originalJson = original.toJSONString

    // 一级节点
    try {
      val paramData = original.getJSONObject("param_data")
      // 清洗PARAM_DATA
      CLEAN_PARAM_DATA.cleansing(map, paramData)
      CLEAN_PARAM_ENV.cleansing(map, original.getJSONObject(PARAM_ENV))
      CLEAN_PARAM_EVENT.cleansing(map, original.getJSONObject(PARAM_EVENT))

      val eventCategory = paramData.getString(DATA_CATEGORY)
      eventCategory match {
        case EVENT_ROLE =>
          CLEAN_PARAM_USER.cleansing(map, original.getJSONObject(PARAM_USER))
          CLEAN_PARAM_ROLE.cleansing(map, original.getJSONObject(PARAM_ROLE))
        case EVENT_USER =>
          CLEAN_PARAM_USER.cleansing(map, original.getJSONObject(PARAM_USER))
        case EVENT_APP =>
        case EVENT_GUILD =>
          CLEAN_PARAM_GUILD.cleansing(map, original.getJSONObject(PARAM_GUILD))
        case _ =>
          throw new IllegalStateException("Unsupported eventCategory: " + eventCategory)
      }
      conversionEventGame("info", map)
    } catch {
      case e: Exception => conversionEventGame("info", map, originalJson, e)
    }
  }

  def conversionEventGame(info: String, map: mutable.Map[String, String]): EventKafkaGame = {
    conversionEventGame(info, map, null, null)
  }

  def conversionEventGame(info: String, map: mutable.Map[String, String], originalJson: String, ex: Exception): EventKafkaGame = {
    val error = ex != null
    if (error) {
      map.put("errorMsg", ex.getClass.getSimpleName + "." + ex.getMessage)
      map.put("originalJson", originalJson)
    }

    map.put("default", "none")
    map.put("defaultUnix", "631123200")

    // 根据info和map中的值进行数据填充
    val id = ""
    val appId = map.remove(DATA_APP_ID).orElse(map.get("default")).get
    val dataUnix = map.remove(DATA_DATA_UNIX).orElse(map.get("defaultUnix")).get.toInt
    val dt = Instant.ofEpochMilli(
      dataUnix * 1000L
    ).atZone(ZoneOffset.ofHours(8)).toLocalDate.toString

    val eventCategory = map.remove(DATA_CATEGORY).orElse(map.get("default")).get
    val eventType = map.remove(EVENT_T0_S).orElse(map.get("default")).get

    val platform = map.remove(DATA_PLATFORM).orElse(map.get("default")).get
    if (platform.isEmpty) map.get("default")
    val channel = map.remove(DATA_CHANNEL).orElse(map.get("default")).get
    if (channel.isEmpty) map.get("default")
    val region = map.remove(DATA_REGION).orElse(map.get("default")).get
    if (region.isEmpty) map.get("default")
    val server = map.remove(DATA_SERVER).orElse(map.get("default")).get
    if (server.isEmpty) map.get("default")

    val did = map.remove(ENV_DEVICE_ID).orElse(map.get("default")).get

    val userId = map.remove(USER_USER_ID).orElse(map.get("default")).get
    val uid = if (DEFAULT_NONE.equals(userId)) DEFAULT_NONE else Joiner.on("|").useForNull(DEFAULT_NONE).join(channel, userId)

    val roleId = map.remove(ROLE_ROLE_ID).orElse(map.get("default")).get
    val rid = if (DEFAULT_NONE.equals(userId) || DEFAULT_NONE.equals(roleId)) DEFAULT_NONE else Joiner.on("|").useForNull(DEFAULT_NONE).join(uid, region, server, roleId)

    val event = Joiner.on(".").join(eventCategory, eventType)
    val ip = map.getOrElse(ENV_IP, EMPTY_STR)
    // 解析IP写入map

    map.remove("default")
    map.remove("defaultUnix")

    EventKafkaGame(if (error) "错误" else "实时/离线",
      appId, dt, event, map, id, platform, channel, region, server, dataUnix, uid, rid, did)
  }
}

case class EventKafkaGame(topic: String, appId: String, dt: String, event: String, map: mutable.Map[String, String], id: String, platform: String, channel: String, region: String, server: String, dataUnix: Int, uid: String, rid: String, did: String)

/**
  * Param_* 相关节点数据清洗抽象实现
  */
abstract class BaseCleanParam extends Serializable {
  val EMPTY_STR: String = ""
  val CUSTOM = "custom"
  val EMPTY_STRINGS: Seq[String] = Seq[String]()
  val paramName: String = this.getClass.getSimpleName

  val DATA_CATEGORY = "category_s"
  val DATA_APP_ID = "app_id_s"
  val DATA_DATA_UNIX = "data_unix"

  val EVENT_ORIGINAL_USER_ID = "user_id_s"
  val EVENT_ORIGINAL_EVENT_S = "event_s"
  val ROLE_ORIGINAL_ROLE_ID = "role_id_s"
  val GUILD_ORIGINAL_GUILD_ID = "guild_id_s"
  val PARAM_EVENT_PREFIX = "t_"
  val PARAM_USER_PREFIX = "u_"
  val PARAM_ROLE_PREFIX = "r_"
  val PARAM_GUILD_PREFIX = "g_"

  /**
    * 必填字段
    * @return 必填字段
    */
  def requiredFields(): Seq[String] = EMPTY_STRINGS

  /**
    * 数据节点前缀
    * @return str
    */
  def paramPrefix: String = EMPTY_STR

  /**
    * 检验字段
    * @param result
    * @param body
    */
  def cleansing(result: mutable.Map[String, String], body: JSONObject): Unit = {
    // 数据必填字段检验
    if (body == null && requiredFields().nonEmpty)
      throw new IllegalArgumentException(format("requiredField[%s.body] is null", ""))

    for (requiredField <- requiredFields()) {
      val o = body.get(requiredField)
      if (o == null || o.toString.isEmpty)
        throw new IllegalArgumentException(format("requiredField[%s.%s] is null or empty", "", requiredField))
    }

    // 自定义参数并入当前对象内
    val custom = body.remove(CUSTOM)
    custom match {
      case m: util.Map[String, AnyRef] =>
        val iterator = m.entrySet().iterator()
        while (iterator.hasNext) {
          val key = iterator.next().getKey
          result.put(key, m.get(key).toString)
        }
    }

    // 合并子节点具体清洗后额外返回数据节点
    this.cleansingParam(body).foreach(x => body.put(x._1, x._2))

    val prefix = paramPrefix
    if (prefix.isEmpty) {
      JSON.toJSONString(body, new NameFilter {
        override def process(o: Any, s: String, o1: Any): String = {
          // TODO 判断类型格式校验
          val value = o1.toString
          if (!value.isEmpty) {
            result.put(s, value)
          }
          s
        }
      })
    } else {
      JSON.toJSONString(body, new NameFilter {
        override def process(o: Any, s: String, o1: Any): String = {
          // TODO 判断类型格式校验
          val value = o1.toString
          if (!value.isEmpty) {
            result.put(paramPrefix + s, value)
          }
          s
        }
      })
    }
  }

  /**
    * 子节点清洗后返回额外数据
    * @param body 当前节点原始数据 body
    * @return
    */
  def cleansingParam(body: JSONObject): mutable.Map[String, String] = mutable.Map[String, String]()
}

class CleanParamData extends BaseCleanParam {
  override def requiredFields(): Seq[String] = Seq[String](DATA_APP_ID, DATA_DATA_UNIX)
}

class CleanParamEvent extends BaseCleanParam {
  val ACTIVITY_4 = "activity_4"
  val DEFAULT_NONE = "none"
  val EVENT_T1_S = PARAM_EVENT_PREFIX + "t1_s"
  val EVENT_T2_S = PARAM_EVENT_PREFIX + "t2_s"
  val EVENT_T3_S = PARAM_EVENT_PREFIX + "t3_s"
  val EVENT_T0_S: String = PARAM_EVENT_PREFIX + "t0_s"
  val EVENT_UNIQUE_T1_S = PARAM_EVENT_PREFIX + "t1_unique_s"
  val EVENT_UNIQUE_T2_S = PARAM_EVENT_PREFIX + "t2_unique_s"
  val EVENT_UNIQUE_T3_S = PARAM_EVENT_PREFIX + "t3_unique_s"

  val REPLACEMENT = "%"
  val CONSTRAINT_LENGTH = 50
  val SEPARATOR_REGEX: Pattern = Pattern.compile("[/|.|:]")
  val SPLIT_EVENT_TYPE_CHAIN = "_"
  val SPLIT_EVENT_CHAIN_PREFIX: String = PARAM_EVENT_PREFIX + "t"

  override def cleansingParam(body: JSONObject): mutable.Map[String, String] = {
    // 解析event_s数据
    val map = SpecialString.eventChainAnalyser(body.getJSONArray(EVENT_ORIGINAL_EVENT_S))
    body.remove(EVENT_ORIGINAL_EVENT_S)
    // 特殊处理 活动加唯一KEY
    if (ACTIVITY_4 == map.get(EVENT_T0_S)) {
      val t1 = map.getOrElse(EVENT_T1_S, DEFAULT_NONE)
      val t2 = map.getOrElse(EVENT_T2_S, DEFAULT_NONE)
      val t3 = map.getOrElse(EVENT_T3_S, DEFAULT_NONE)
      map.put(EVENT_UNIQUE_T1_S, t1)
      val t2Unique = t1 + ">" + t2
      map.put(EVENT_UNIQUE_T2_S, t2Unique)
      val t3Unique = t2Unique + ">" + t3
      map.put(EVENT_UNIQUE_T3_S, t3Unique)
    }
    val iterator = body.entrySet().iterator()
    while (iterator.hasNext) {
      val key = iterator.next().getKey
      map.put(PARAM_EVENT_PREFIX + key, body.getString(key))
    }
    map
  }
}

class CleanParamEvn extends BaseCleanParam {}

class CleanParamUser extends BaseCleanParam {
  override def requiredFields(): Seq[String] = Seq[String](EVENT_ORIGINAL_USER_ID)

  override def paramPrefix: String = PARAM_USER_PREFIX
}

class CleanParamRole extends BaseCleanParam {
  override def requiredFields(): Seq[String] = Seq[String](ROLE_ORIGINAL_ROLE_ID)

  override def paramPrefix: String = PARAM_ROLE_PREFIX
}

class CleanParamGuild extends BaseCleanParam {
  override def requiredFields(): Seq[String] = Seq[String](GUILD_ORIGINAL_GUILD_ID)

  override def paramPrefix: String = PARAM_GUILD_PREFIX
}

object SpecialString extends Serializable {
  val REPLACEMENT = "%"
  val CONSTRAINT_LENGTH = 50
  val SEPARATOR_REGEX: Pattern = Pattern.compile("[/|.|:]")
  val SPLIT_EVENT_TYPE_CHAIN = "_"
  val PARAM_EVENT_PREFIX = "t_"
  val SPLIT_EVENT_CHAIN_PREFIX: String = PARAM_EVENT_PREFIX + "t"
  val loop = new Breaks

  def eventChainAnalyser(events: java.util.List[Object]): mutable.Map[String, String] = {
    if (events == null || events.isEmpty) {
      throw new IllegalArgumentException("ParamEvent#event_s must be not empty")
    }

    val eventLen = events.size
    val eventType = events.get(0).toString

    if (SEPARATOR_REGEX.matcher(eventType).find()) {
      throw new IllegalArgumentException("eventType contains separator str")
    }

    val splitEventType = eventType.split(SPLIT_EVENT_TYPE_CHAIN)
    val size = splitEventType(1).toInt
    val sizeTmp = size + splitEventType.length - 1
    if (size < 3 && sizeTmp != eventLen) {
      throw new IllegalArgumentException("ParamEvent#event_s parse error , event_s = " + events)
    }

    val map = mutable.Map[String, String]()
    loop.breakable {
      for (i <- (0 until splitEventType.length).reverse) {
        if (i == 1) {
          for (j <- 0 to size) {
            map.put(SPLIT_EVENT_CHAIN_PREFIX + j + "_s", events.get(j).toString)
          }
          loop.break()
        }
        val index = i + size - 1
        map.put(SPLIT_EVENT_CHAIN_PREFIX + index + SPLIT_EVENT_TYPE_CHAIN + splitEventType(i), events.get(index).toString)
      }
    }

    map
  }
}

