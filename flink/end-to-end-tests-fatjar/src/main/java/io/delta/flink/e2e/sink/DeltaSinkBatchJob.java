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
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.restartstrategy.RestartStrategies.RestartStrategyConfiguration;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import static io.delta.flink.e2e.sink.DeltaSinkFactory.createDeltaSink;

public class DeltaSinkBatchJob {

    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        int inputRecordsCount = parameters.getInt("input-records");
        String tablePath = parameters.get("delta-table-path");
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

        getPipeline(env, tablePath, isTablePartitioned, inputRecordsCount, triggerFailover);
        env.execute(testName);
    }

    public static void getPipeline(StreamExecutionEnvironment env,
                                   String deltaTablePath,
                                   boolean isTablePartitioned,
                                   int inputRecordsCount,
                                   boolean triggerFailover) {
        DeltaSinkInternal<RowData> deltaSink = createDeltaSink(deltaTablePath, isTablePartitioned);

        env.fromCollection(RowDataListGenerator.getTestRowData(inputRecordsCount))
            .setParallelism(1)
            .map(triggerFailover ?
                new FailingMapFunction(inputRecordsCount / env.getParallelism() / 2)
                : new IdentityMapFunction()
            )
            .sinkTo(deltaSink);
    }

    private static final class IdentityMapFunction implements MapFunction<RowData, RowData> {
        @Override
        public RowData map(RowData value) {
            return value;
        }
    }

    private static final class FailingMapFunction extends RichMapFunction<RowData, RowData> {

        private final int failAfterRecordCount;

        private int count;

        FailingMapFunction(int failAfterRecordCount) {
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
}
