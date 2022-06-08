package io.delta.flink.e2e.sink;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;

public class DeltaStreamingExecutionTestSource
    extends RichParallelSourceFunction<RowData>
    implements CheckpointListener, CheckpointedFunction {

    private static final Map<String, CountDownLatch> LATCH_MAP = new ConcurrentHashMap<>();
    protected static final double FAILOVER_RATIO = 0.4;

    private final String latchId;

    private final int numberOfRecords;

    /**
     * Whether the test is executing in a scenario that induces a failover. This doesn't mean
     * that this source induces the failover.
     */
    private final boolean isFailoverScenario;

    private ListState<Integer> nextValueState;

    private int nextValue;

    private volatile boolean isCanceled;

    private volatile boolean snapshottedAfterAllRecordsOutput;

    private volatile boolean isWaitingCheckpointComplete;

    private volatile boolean hasCompletedCheckpoint;

    private volatile boolean isLastCheckpointInterval;

    DeltaStreamingExecutionTestSource(
        String latchId, int numberOfRecords, boolean isFailoverScenario) {
        this.latchId = latchId;
        this.numberOfRecords = numberOfRecords;
        this.isFailoverScenario = isFailoverScenario;
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
        if (isFailoverScenario && getRuntimeContext().getAttemptNumber() == 0) {
            // In the first execution, we first send a part of record...
            sendRecordsUntil((int) (numberOfRecords * FAILOVER_RATIO * 0.5), ctx);

            // Wait till the first part of data is committed.
            while (!hasCompletedCheckpoint) {
                Thread.sleep(50);
            }

            // Then we write the second part of data...
            sendRecordsUntil((int) (numberOfRecords * FAILOVER_RATIO), ctx);

            // And then trigger the failover.
            if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
                throw new RuntimeException("Designated Exception");
            } else {
                while (true) {
                    Thread.sleep(50);
                }
            }
        } else {
            // If we are not going to trigger failover or we have already triggered failover,
            // run until finished.
            sendRecordsUntil(numberOfRecords, ctx);

            isWaitingCheckpointComplete = true;
            CountDownLatch latch = LATCH_MAP.get(latchId);
            latch.await();
        }
    }

    private void sendRecordsUntil(int targetNumber, SourceContext<RowData> ctx) {
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
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        nextValueState.update(Collections.singletonList(nextValue));
        if (isWaitingCheckpointComplete) {
            snapshottedAfterAllRecordsOutput = true;
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        if (isWaitingCheckpointComplete && snapshottedAfterAllRecordsOutput
            && isLastCheckpointInterval) {
            CountDownLatch latch = LATCH_MAP.get(latchId);
            latch.countDown();
        }

        if (isWaitingCheckpointComplete && snapshottedAfterAllRecordsOutput
            && !isLastCheckpointInterval) {
            // we set the job to run for one additional checkpoint interval to avoid any
            // premature job termination and race conditions
            isLastCheckpointInterval = true;
        }

        hasCompletedCheckpoint = true;
    }

    @Override
    public void cancel() {
        isCanceled = true;
    }
}
