package io.delta.flink.e2e.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.table.data.RowData;

public class IdentityMapFunction implements MapFunction<RowData, RowData> {
    @Override
    public RowData map(RowData value) {
        return value;
    }
}
