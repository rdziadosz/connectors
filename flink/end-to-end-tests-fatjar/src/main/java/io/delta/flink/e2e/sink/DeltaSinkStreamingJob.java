/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.flink.e2e.sink;

import io.delta.flink.sink.internal.DeltaSinkInternal;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.restartstrategy.RestartStrategies.RestartStrategyConfiguration;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import static io.delta.flink.e2e.sink.DeltaSinkFactory.createDeltaSink;

public class DeltaSinkStreamingJob {

    private static final int EXPECTED_CHECKPOINTS = 25;

    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        int inputRecordsCount = parameters.getInt("input-records");
        boolean isTablePartitioned = parameters.getBoolean("is-table-partitioned");
        boolean triggerFailover = parameters.getBoolean("trigger-failover");
        String tablePath = parameters.get("delta-table-path");
        String testName = parameters.get("test-name");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        getPipeline(env, tablePath, isTablePartitioned, triggerFailover, inputRecordsCount);
        env.execute(testName);
    }

    public static void getPipeline(StreamExecutionEnvironment env,
                                   String deltaTablePath,
                                   boolean isTablePartitioned,
                                   boolean triggerFailover,
                                   int inputRecordsCount) {
        RestartStrategyConfiguration restartStrategyConfiguration = triggerFailover
            ? RestartStrategies.fixedDelayRestart(1, Time.seconds(10))
            : RestartStrategies.noRestart();

        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(100L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(100L);
        env.getCheckpointConfig().setCheckpointTimeout(10_000L);
        env.setRestartStrategy(restartStrategyConfiguration);
        env.disableOperatorChaining();

        DeltaSinkInternal<RowData> deltaSink = createDeltaSink(deltaTablePath, isTablePartitioned);

        env.addSource(new CheckpointCountingSource(inputRecordsCount, EXPECTED_CHECKPOINTS,
                triggerFailover))
            .sinkTo(deltaSink);
    }

}
