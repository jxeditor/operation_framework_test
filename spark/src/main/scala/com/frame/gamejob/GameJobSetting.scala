package com.frame.gamejob

import com.frame.tools.time.TimesDtTools

/**
  * 任务运行设置 ，开始结束时间描述为 [startDt,endDt)
  * JSON default e.g:
  * {
  * "startDt": "2019-08-16",
  * "endDt": "2019-08-17",
  * "calculatePriority": -1,
  * "includeAppIds": [],
  * "excludeAppIds": [],
  * "includeTasks": [],
  * "excludeTasks": [],
  * "elasticJobSetting": {
  * "includeTypes": [],
  * "excludeTypes": [],
  * "deleteData": false,
  * "selectAll": false
  * }
  * }
  * @param startDt           开始时间 dt(yyyy-MM-dd),默认为昨天 dt
  * @param endDt             结束时间 dt(yyyy-MM-dd),默认为今天 dt
  * @param calculatePriority 处理优先级 ， -1 表示忽略
  * @param includeAppIds     指定 apps
  * @param excludeAppIds     排除 apps
  * @param includeTasks      指定 tasks task 类名作为过滤
  * @param excludeTasks      排除 tasks task 类名作为过滤
  * @param elasticJobSetting es job 设置
  * @author XiaShuai on 2020/4/8.
  */
case class GameJobSetting(startDt: String = TimesDtTools.dt(TimesDtTools.nowDt(), -1), // 默认为昨天 dt
                          endDt: String = TimesDtTools.nowDt(), // 默认为今天 dt
                          calculatePriority: Int = -1, // 处理优先级 ， -1 表示忽略
                          includeAppIds: Array[String] = Array.empty,
                          excludeAppIds: Array[String] = Array.empty,
                          includeTasks: Array[String] = Array.empty,
                          excludeTasks: Array[String] = Array.empty,
                          elasticJobSetting: ElasticJobSetting = ElasticJobSetting(),
                          obsJobSetting: ObsJobSetting = ObsJobSetting())

/**
  * es job 设置
  */
case class ElasticJobSetting(includeTypes: Array[String] = Array.empty,
                             excludeTypes: Array[String] = Array.empty,
                             deleteData: Boolean = false,
                             selectAll: Boolean = false)

/**
  * obs job 设置
  * @param warehouseDb    数据库路径
  * @param warehouseTb    表路径
  * @param warehouseEvent event路径
  */
case class ObsJobSetting(warehouseDb: String = null, warehouseTb: String = null, warehouseEvent: String = null)
