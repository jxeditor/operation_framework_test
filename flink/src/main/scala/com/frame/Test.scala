package com.frame

import org.apache.flink.table.api.{EnvironmentSettings, SqlParserException, TableConfig}
import org.apache.flink.table.catalog.{CatalogManager, FunctionCatalog, GenericInMemoryCatalog}
import org.apache.flink.table.planner.calcite.{CalciteParser, FlinkPlannerImpl}
import org.apache.flink.table.planner.delegation.{ParserImpl, PlannerContext}
import java.util.function.{Supplier => JSupplier}

import org.apache.calcite.jdbc.CalciteSchemaBuilder.asRootSchema
import org.apache.calcite.plan.{ConventionTraitDef, RelTrait, RelTraitDef}
import org.apache.flink.table.catalog.hive.HiveCatalog
import org.apache.flink.table.module.ModuleManager
import org.apache.flink.table.planner.catalog.CatalogManagerCalciteSchema
import org.apache.flink.table.planner.plan.`trait`.{AccModeTraitDef, FlinkRelDistributionTraitDef, MiniBatchIntervalTraitDef, UpdateAsRetractionTraitDef}

import scala.collection.JavaConverters._

/**
  * @author XiaShuai on 2020/5/4.
  */
object Test {

  def main(args: Array[String]): Unit = {
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build

    // Catalog
    val catalogManager = new CatalogManager(settings.getBuiltInCatalogName, new GenericInMemoryCatalog(settings.getBuiltInCatalogName, settings.getBuiltInDatabaseName))
    val hiveCatalog = new HiveCatalog("test", "default",
      "F:\\operation_framework_test\\flink\\src\\main\\resources\\hive_conf", "2.1.1")
    catalogManager.registerCatalog("catalog", hiveCatalog)
    catalogManager.setCurrentCatalog("catalog")

    val currentCatalogName = catalogManager.getCurrentCatalog
    val currentDatabase = catalogManager.getCurrentDatabase
    val config = new TableConfig

    val moduleManager = new ModuleManager
    val functionCatalog = new FunctionCatalog(config, catalogManager, moduleManager)

    // PlannerContext执行器上下文
    val plannerContext: PlannerContext =
      new PlannerContext(
        config,
        functionCatalog,
        catalogManager,
        asRootSchema(new CatalogManagerCalciteSchema(catalogManager, settings.isStreamingMode)),
        getTraitDefs.toList.asJava
      )

    // 具现化执行器
    val planner = plannerContext.createFlinkPlanner(currentCatalogName, currentDatabase)

    // 具现化SQL解析器
    val parserImpl = new ParserImpl(
      catalogManager,
      new JSupplier[FlinkPlannerImpl] {
        override def get(): FlinkPlannerImpl = planner
      },
      new JSupplier[CalciteParser] {
        override def get(): CalciteParser = plannerContext.createCalciteParser()
      }
    )

    val operations = parserImpl.parse("select * from test")
    val operation = operations.get(0)


  }

  def getTraitDefs: Array[RelTraitDef[_ <: RelTrait]] = {
    Array(
      ConventionTraitDef.INSTANCE,
      FlinkRelDistributionTraitDef.INSTANCE,
      MiniBatchIntervalTraitDef.INSTANCE,
      UpdateAsRetractionTraitDef.INSTANCE,
      AccModeTraitDef.INSTANCE)
  }


}
