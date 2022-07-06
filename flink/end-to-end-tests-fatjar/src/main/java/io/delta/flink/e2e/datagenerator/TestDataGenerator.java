package io.delta.flink.e2e.datagenerator;

import java.io.Serializable;

import org.apache.flink.table.data.RowData;

public interface TestDataGenerator extends Serializable {
    RowData get(int index);
}
