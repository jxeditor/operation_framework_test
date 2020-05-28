package com.frame;


import org.apache.flink.table.api.*;

import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelTraitDef;

import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.expressions.TableReferenceExpression;
import org.apache.flink.table.operations.CatalogQueryOperation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.utils.OperationTreeBuilder;
import org.apache.flink.table.planner.catalog.CatalogManagerCalciteSchema;

import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.operations.Operation;

import org.apache.flink.table.planner.calcite.CalciteParser;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.delegation.ParserImpl;
import org.apache.flink.table.planner.delegation.PlannerContext;
import org.apache.flink.table.planner.plan.trait.AccModeTraitDef;
import org.apache.flink.table.planner.plan.trait.FlinkRelDistributionTraitDef;
import org.apache.flink.table.planner.plan.trait.MiniBatchIntervalTraitDef;
import org.apache.flink.table.planner.plan.trait.UpdateAsRetractionTraitDef;

import static org.apache.calcite.jdbc.CalciteSchemaBuilder.asRootSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * @author XiaShuai on 2020/5/5.
 */
public class Test1 {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        // Catalog
        CatalogManager catalogManager = new CatalogManager(settings.getBuiltInCatalogName(), new GenericInMemoryCatalog(settings.getBuiltInCatalogName(), settings.getBuiltInDatabaseName()));
        HiveCatalog hiveCatalog = new HiveCatalog("test", "default",
                "F:\\operation_framework_test\\flink\\src\\main\\resources\\hive_conf", "2.1.1");
        catalogManager.registerCatalog("catalog", hiveCatalog);
        catalogManager.setCurrentCatalog("catalog");

        String currentCatalogName = catalogManager.getCurrentCatalog();
        String currentDatabase = catalogManager.getCurrentDatabase();
        TableConfig config = new TableConfig();

        ModuleManager moduleManager = new ModuleManager();
        FunctionCatalog functionCatalog = new FunctionCatalog(config, catalogManager, moduleManager);

        // PlannerContext执行器上下文
        PlannerContext plannerContext =
                new PlannerContext(
                        config,
                        functionCatalog,
                        catalogManager,
                        asRootSchema(new CatalogManagerCalciteSchema(catalogManager, settings.isStreamingMode())),
                        getTraitDefs()
                );

        // 具现化执行器
        FlinkPlannerImpl planner = plannerContext.createFlinkPlanner(currentCatalogName, currentDatabase);

        // 具现化SQL解析器
        ParserImpl parserImpl = new ParserImpl(
                catalogManager,
                new Supplier<FlinkPlannerImpl>() {
                    @Override
                    public FlinkPlannerImpl get() {
                        return planner;
                    }
                },
                new Supplier<CalciteParser>() {
                    @Override
                    public CalciteParser get() {
                        return plannerContext.createCalciteParser();
                    }
                }
        );

        System.out.println(config.getSqlDialect());

        // 获取操作,判断是否是QueryOperation
        List<Operation> operations = parserImpl.parse("select * from test");
        Operation operation = operations.get(0);
        System.out.println(operation.asSummaryString());

        OperationTreeBuilder operationTreeBuilder = OperationTreeBuilder.create(
                config,
                functionCatalog,
                path -> {
                    try {

                        UnresolvedIdentifier unresolvedIdentifier = parserImpl.parseIdentifier(path);
                        Optional<CatalogQueryOperation> catalogQueryOperation = scanInternal(unresolvedIdentifier, catalogManager);
                        return catalogQueryOperation.map(t -> new TableReferenceExpression(path, t));
                    } catch (SqlParserException ex) {
                        return Optional.empty();
                    }
                },
                settings.isStreamingMode()
        );

        TableImpl table = TableImpl.createTable(TableEnvironment.create(settings),
                (QueryOperation) operation,
                operationTreeBuilder,
                functionCatalog
        );

        Table rid = table.select("rid");

        System.out.println(operationTreeBuilder.toString());



    }

    public static List<RelTraitDef> getTraitDefs() {
        ArrayList<RelTraitDef> list = new ArrayList<>();
        list.add(ConventionTraitDef.INSTANCE);
        list.add(FlinkRelDistributionTraitDef.INSTANCE());
        list.add(MiniBatchIntervalTraitDef.INSTANCE());
        list.add(UpdateAsRetractionTraitDef.INSTANCE());
        list.add(AccModeTraitDef.INSTANCE());
        return list;
    }

    private static Optional<CatalogQueryOperation> scanInternal(UnresolvedIdentifier identifier, CatalogManager catalogManager) {
        ObjectIdentifier tableIdentifier = catalogManager.qualifyIdentifier(identifier);

        return catalogManager.getTable(tableIdentifier)
                .map(t -> new CatalogQueryOperation(tableIdentifier, t.getTable().getSchema()));
    }

}
