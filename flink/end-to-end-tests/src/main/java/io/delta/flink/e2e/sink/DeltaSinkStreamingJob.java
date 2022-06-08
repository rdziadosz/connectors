package io.delta.flink.e2e.sink;

import io.delta.flink.sink.internal.DeltaSinkInternal;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import static io.delta.flink.e2e.sink.DeltaSinkTestUtils.createDeltaSink;

public class DeltaSinkStreamingJob {

    public static void getPipeline(StreamExecutionEnvironment env,
                                   DeltaSinkStreamingJobParameters parameters) {
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(100L, CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.disableOperatorChaining();

        DeltaSinkInternal<RowData> deltaSink = createDeltaSink(
            parameters.getDeltaTablePath(),
            parameters.isPartitioned()
        );

        env.addSource(new SimpleTestSource(parameters.getInputRecordsCount()))
            .setParallelism(parameters.getSourceParallelism())
            .sinkTo(deltaSink)
            .setParallelism(parameters.getSinkParallelism());
    }
}
