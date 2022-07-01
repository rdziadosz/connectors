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
        String testName = parameters.get("test-name");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        getPipeline(env, tablePath, isTablePartitioned, inputRecordsCount);
        env.execute(testName);
    }

    public static void getPipeline(StreamExecutionEnvironment env,
                                   String deltaTablePath,
                                   boolean isTablePartitioned,
                                   int inputRecordsCount) {
        DeltaSinkInternal<RowData> deltaSink = createDeltaSink(deltaTablePath, isTablePartitioned);

        env.fromCollection(RowDataListGenerator.getTestRowData(inputRecordsCount))
            .setParallelism(1)
            .sinkTo(deltaSink)
            .setParallelism(3);
    }
}
