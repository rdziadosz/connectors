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

import java.time.Duration;

import io.delta.flink.e2e.DeltaConnectorEndToEndTestBase;
import io.delta.flink.e2e.client.parameters.JobParameters;
import io.delta.flink.e2e.client.parameters.JobParametersBuilder;
import io.delta.flink.e2e.utils.HadoopConfig;
import io.delta.flink.utils.TestParquetReader;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import static io.delta.flink.e2e.assertions.DeltaLogAssertions.assertThat;

import io.delta.standalone.DeltaLog;

@RunWith(Parameterized.class)
@DisplayNameGeneration(DisplayNameGenerator.IndicativeSentences.class)
class DeltaSinkStreamingEndToEndTest extends DeltaConnectorEndToEndTestBase {

    private static final int INPUT_RECORDS = 10_000;
    private static final int PARALLELISM = 3;
    private static final String JOB_MAIN_CLASS = "io.delta.flink.e2e.sink.DeltaSinkStreamingJob";


    @DisplayName("Connector in streaming mode should add new records to the Delta Table")
    @ParameterizedTest(name = "partitioned table: {1}; failover: {0}")
    @CsvSource(value = {"false,false", "true,false", "false,true", "true,true"})
    void shouldAddNewRecords(boolean triggerFailover, boolean isPartitioned) throws Exception {
        // GIVEN
        String tablePath = isPartitioned ? getPartitionedTablePath() : getNonPartitionedTablePath();
        DeltaLog deltaLog = DeltaLog.forTable(HadoopConfig.get(), tablePath);
        // AND
        long initialDeltaVersion = deltaLog.snapshot().getVersion();
        int initialRecordCount = TestParquetReader.readAndValidateAllTableRecords(deltaLog);
        // AND
        JobParameters jobParameters = streamingJobParameters()
            .withDeltaTablePath(tablePath)
            .withTablePartitioned(isPartitioned)
            .withTriggerFailover(triggerFailover)
            .build();

        // WHEN
        jobID = flinkClient.run(jobParameters);
        wait(Duration.ofMinutes(3));

        // THEN
        assertThat(deltaLog)
            .sinceVersion(initialDeltaVersion)
            .hasRecordCountInParquetFiles(initialRecordCount + INPUT_RECORDS * PARALLELISM)
            .hasNewRecordCountInOperationMetrics(INPUT_RECORDS * PARALLELISM)
            .metricsNewFileCountMatchesSnapshotNewFileCount()
            .hasPositiveNumOutputBytesInEachVersion();
    }


    @DisplayName("Connector in streaming mode should create Delta checkpoints")
    @ParameterizedTest(name = "partitioned table: {1}; failover: {0}")
    @CsvSource(value = {"false,false", "true,false", "false,true", "true,true"})
    void shouldCreateCheckpoints(boolean triggerFailover, boolean isPartitioned) throws Exception {
        // GIVEN
        String tablePath = isPartitioned ? getPartitionedTablePath() : getNonPartitionedTablePath();
        DeltaLog deltaLog = DeltaLog.forTable(HadoopConfig.get(), tablePath);
        // AND
        long initialDeltaVersion = deltaLog.snapshot().getVersion();
        // AND
        JobParameters jobParameters = streamingJobParameters()
            .withDeltaTablePath(tablePath)
            .withTablePartitioned(isPartitioned)
            .withTriggerFailover(triggerFailover)
            .build();

        // WHEN
        jobID = flinkClient.run(jobParameters);
        wait(Duration.ofMinutes(3));

        // THEN
        assertThat(deltaLog)
            .sinceVersion(initialDeltaVersion)
            .hasCheckpointsCount(2)
            .hasLastCheckpointFile();
    }

    private JobParametersBuilder streamingJobParameters() {
        return JobParametersBuilder.builder()
            .withName(getTestDisplayName())
            .withJarId(jarId)
            .withEntryPointClassName(JOB_MAIN_CLASS)
            .withParallelism(PARALLELISM)
            .withInputRecords(INPUT_RECORDS);
    }

}
