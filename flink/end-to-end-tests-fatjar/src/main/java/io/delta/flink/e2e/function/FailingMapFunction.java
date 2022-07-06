package io.delta.flink.e2e.function;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.table.data.RowData;

public class FailingMapFunction extends RichMapFunction<RowData, RowData> {

    private final int failAfterRecordCount;

    private int count;

    public FailingMapFunction(int failAfterRecordCount) {
        this.failAfterRecordCount = failAfterRecordCount;
        this.count = 0;
    }

    @Override
    public RowData map(RowData value) {
        count++;
        if (count == failAfterRecordCount &&
            getRuntimeContext().getIndexOfThisSubtask() == 0 &&
            getRuntimeContext().getAttemptNumber() == 0) {
            throw new RuntimeException("Designated Exception");
        }
        return value;
    }
}
