package io.delta.flink.e2e.sink;

import java.util.Collections;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;

public class SimpleTestSource
    extends RichParallelSourceFunction<RowData>
    implements CheckpointedFunction {

    private final int numberOfRecords;
    private ListState<Integer> nextValueState;
    private int nextValue;
    private volatile boolean isCanceled;

    SimpleTestSource(int numberOfRecords) {
        this.numberOfRecords = numberOfRecords;
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        nextValueState =
            context.getOperatorStateStore()
                .getListState(new ListStateDescriptor<>("nextValue", Integer.class));

        if (nextValueState.get() != null && nextValueState.get().iterator().hasNext()) {
            nextValue = nextValueState.get().iterator().next();
        }
    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        sendRecordsUntil(numberOfRecords, ctx);
        idle();
    }

    private void sendRecordsUntil(int targetNumber, SourceContext<RowData> ctx)
        throws InterruptedException {
        while (!isCanceled && nextValue < targetNumber) {
            synchronized (ctx.getCheckpointLock()) {
                RowData row = DeltaSinkTestUtils.CONVERTER.toInternal(
                    Row.of(
                        String.valueOf(nextValue),
                        String.valueOf((nextValue + nextValue)),
                        nextValue)
                );
                ctx.collect(row);
                nextValue++;
            }
            Thread.sleep(1L);
        }
    }

    private void idle() throws InterruptedException {
        while (!isCanceled) {
            Thread.sleep(10L);
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        nextValueState.update(Collections.singletonList(nextValue));
    }

    @Override
    public void cancel() {
        isCanceled = true;
    }
}
