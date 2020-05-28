//package com.frame.test.batch.hbase1;
//
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.api.java.typeutils.RowTypeInfo;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.TableSchema;
//import org.apache.flink.table.functions.AsyncTableFunction;
//import org.apache.flink.table.functions.TableFunction;
//import org.apache.flink.table.sources.LookupableTableSource;
//import org.apache.flink.table.sources.StreamTableSource;
//import org.apache.flink.table.sources.TableSource;
//import org.apache.flink.table.types.DataType;
//import org.apache.flink.table.types.utils.TypeConversions;
//import org.apache.flink.types.Row;
//
//public class HBaseAsyncLookupTableSource implements StreamTableSource<Row>,LookupableTableSource<Row> {
//    private final String tableName;
//    private final String[] fieldNames;
//    private final TypeInformation[] fieldTypes;
//    private final String rowkey;
//    private final String zkQuorum;
//
//    public HBaseAsyncLookupTableSource(String tableName, String[] fieldNames, TypeInformation[] fieldTypes, String rowkey, String zkQuorum) {
//        this.tableName = tableName;
//        this.fieldNames = fieldNames;
//        this.fieldTypes = fieldTypes;
//        this.rowkey = rowkey;
//        this.zkQuorum = zkQuorum;
//    }
//
//    //同步
//    public TableFunction<Row> getLookupFunction(String[] strings) {
//        return null;
//    }
//
//    //异步
//    public AsyncTableFunction<Row> getAsyncLookupFunction(String[] strings) {
//        return HBaseAsyncLookupFunction.Builder.getBuilder()
//                .withTableName(tableName)
//                .withFieldNames(fieldNames)
//                .withFieldTypes(fieldTypes)
//                .withRowkey(rowkey)
//                .withZkQuorum(zkQuorum).build();
//    }
//
//    //开启异步
//    public boolean isAsyncEnabled() {
//        return true;
//    }
//
//    @Override
//    public DataType getProducedDataType() {
//        return TypeConversions.fromLegacyInfoToDataType(new RowTypeInfo(fieldTypes, fieldNames));
//    }
//
//    public DataStream<Row> getDataStream(StreamExecutionEnvironment streamExecutionEnvironment) {
//        throw new UnsupportedOperationException("do not support getDataStream");
//    }
//
//    public TableSchema getTableSchema() {
//        return TableSchema.builder()
//                .fields(fieldNames, TypeConversions.fromLegacyInfoToDataType(fieldTypes))
//                .build();
//    }
//
//    public static final class Builder {
//        private String tableName;
//        private String[] fieldNames;
//        private TypeInformation[] fieldTypes;
//        private String rowkey;
//        private String zkQuorum;
//
//        private Builder() {
//        }
//
//        public static Builder newBuilder() {
//            return new Builder();
//        }
//
//        public Builder withFieldNames(String[] fieldNames) {
//            this.fieldNames = fieldNames;
//            return this;
//        }
//
//        public Builder withFieldTypes(TypeInformation[] fieldTypes) {
//            this.fieldTypes = fieldTypes;
//            return this;
//        }
//
//        public Builder withTableName(String tableName) {
//            this.tableName = tableName;
//            return this;
//        }
//
//        public Builder withRowkey(String rowkey) {
//            this.rowkey = rowkey;
//            return this;
//        }
//
//        public Builder withZkQuorum(String zkQuorum) {
//            this.zkQuorum = zkQuorum;
//            return this;
//        }
//
//
//        public HBaseAsyncLookupTableSource build() {
//            return new HBaseAsyncLookupTableSource(tableName, fieldNames, fieldTypes, rowkey, zkQuorum);
//        }
//    }
//}
