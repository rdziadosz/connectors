package io.delta.flink.e2e.source;

import io.delta.flink.e2e.function.FailingMapFunction;
import io.delta.flink.e2e.function.IdentityMapFunction;
import io.delta.flink.source.DeltaSource;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.restartstrategy.RestartStrategies.RestartStrategyConfiguration;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;

public class DeltaSourceBatchJob {

    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        String tablePath = parameters.get("delta-table-path");
        String outputPath = parameters.get("output-path");
        boolean isTablePartitioned = parameters.getBoolean("is-table-partitioned");
        boolean triggerFailover = parameters.getBoolean("trigger-failover");
        String testName = parameters.get("test-name");

        RestartStrategyConfiguration restartStrategyConfiguration = triggerFailover
            ? RestartStrategies.fixedDelayRestart(1, Time.seconds(10))
            : RestartStrategies.noRestart();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setRestartStrategy(restartStrategyConfiguration);
        env.disableOperatorChaining();

        getPipeline(env, tablePath, outputPath, triggerFailover);
        env.execute(testName);
    }

    public static void getPipeline(StreamExecutionEnvironment env,
                                   String deltaTablePath,
                                   String outputPath,
                                   boolean triggerFailover) {
        DeltaSource<RowData> deltaSource = DeltaSource
            .forBoundedRowData(new Path(deltaTablePath), HadoopConfig.get())
            .build();

        env.fromSource(deltaSource, WatermarkStrategy.noWatermarks(), "delta-source")
            // FIXME: magic number 10
            .map(triggerFailover ? new FailingMapFunction(10) : new IdentityMapFunction())
            .sinkTo(TestFileSinkFactory.get(outputPath));
    }

}
