package com.frame.test.batch.hbase1;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.types.Row;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class HBaseAsyncLookupFunction extends AsyncTableFunction<Row> {
    private final String tableName;
    private final String[] fieldName;
    private final TypeInformation[] fieldTypes;
    private final String rowkey;
    private final String zkQuorum;
    private transient HBaseClient hBaseClient;

//    public HBaseAsyncLookupFunction(String tableName, String[] fieldName, TypeInformation[] fieldTypes) {
//        this.tableName = tableName;
//        this.fieldName = fieldName;
//        this.fieldTypes = fieldTypes;
//    }

    public HBaseAsyncLookupFunction(String tableName, String[] fieldName, TypeInformation[] fieldTypes, String rowkey, String zkQuorum) {
        this.tableName = tableName;
        this.fieldName = fieldName;
        this.fieldTypes = fieldTypes;
        this.rowkey = rowkey;
        this.zkQuorum = zkQuorum;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        hBaseClient = new HBaseClient(zkQuorum);
    }

    //每一条流数据都会调用此方法进行join
    public void eval(CompletableFuture<Collection<Row>> future,Object... paramas){
        GetRequest get = new GetRequest(tableName,rowkey);
        Deferred<ArrayList<KeyValue>> arrayListDeferred = hBaseClient.get(get);
        arrayListDeferred.addCallback(new Callback<String, ArrayList<KeyValue>>() {
            @Override
            public String call(ArrayList<KeyValue> keyValues) throws Exception {
                String value;
                if (keyValues.size() == 0){
                    value = null;
                }else{
                    StringBuilder valueBuilder = new StringBuilder();
                    for (KeyValue keyValue : keyValues){
                        valueBuilder.append(new String(keyValue.value()));
                    }
                    value = valueBuilder.toString();
                }
                future.complete(Collections.singletonList(Row.of(rowkey,value)));
                return "";
            }
        });
    }

    @Override
    public TypeInformation<Row> getResultType() {
        return new RowTypeInfo(fieldTypes,fieldName);
    }

    public static final class Builder {
        private String tableName;
        private String[] fieldNames;
        private TypeInformation[] fieldTypes;
        private String rowkey;
        private String zkQuorum;

        private Builder() {
        }

        public static Builder getBuilder() {
            return new Builder();
        }

        public Builder withFieldNames(String[] fieldNames) {
            this.fieldNames = fieldNames;
            return this;
        }

        public Builder withFieldTypes(TypeInformation[] fieldTypes) {
            this.fieldTypes = fieldTypes;
            return this;
        }

        public Builder withTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }
        public Builder withRowkey(String rowkey) {
            this.rowkey = rowkey;
            return this;
        }

        public Builder withZkQuorum(String zkQuorum) {
            this.zkQuorum = zkQuorum;
            return this;
        }


        public HBaseAsyncLookupFunction build() {
            return new HBaseAsyncLookupFunction(tableName, fieldNames, fieldTypes,rowkey,zkQuorum);
        }
    }
}
