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

package io.delta.flink.sink;

import java.time.Duration;

import io.delta.flink.jobrunner.JobParameters;
import io.delta.flink.jobrunner.JobParametersBuilder;
import io.delta.flink.utils.DeltaTestUtils;
import io.delta.flink.utils.TestParquetReader;
import org.junit.jupiter.api.Test;
import static io.delta.flink.assertions.DeltaLogAssertions.assertThat;

import io.delta.standalone.DeltaLog;

class DeltaSinkBatchJobEndToEndTest extends DeltaSinkJobEndToEndTestBase {

    private static final int INPUT_RECORDS = 10_000;

    @Test
    void shouldAddNewRecordsToNonPartitionedDeltaTable() throws Exception {
        // GIVEN
        String tablePath = getNonPartitionedTablePath();
        DeltaLog deltaLog = DeltaLog.forTable(DeltaTestUtils.getHadoopConf(), tablePath);
        long initialDeltaVersion = deltaLog.snapshot().getVersion();
        int initialRecordCount = TestParquetReader.readAndValidateAllTableRecords(deltaLog);
        // AND
        JobParameters jobParameters = JobParametersBuilder.builder()
            .withName("[E2E] Sink: add new records in batch mode")
            .withJarPath(getTestArtifactPath())
            .withEntryPointClassName("io.delta.flink.e2e.sink.DeltaSinkBatchJob")
            .withArgument("delta-table-path", tablePath)
            .withArgument("is-table-partitioned", false)
            .withArgument("input-records", INPUT_RECORDS)
            .build();

        // WHEN
        flinkClient.run(jobParameters);
        wait(Duration.ofMinutes(2));

        // THEN
        assertThat(deltaLog)
            .sinceVersion(initialDeltaVersion)
            .hasRecordCountInParquetFiles(initialRecordCount + INPUT_RECORDS)
            .hasNewRecordCountInOperationMetrics(INPUT_RECORDS)
            .metricsNewFileCountMatchesSnapshotNewFileCount()
            .hasPositiveNumOutputBytesInEachVersion();
    }

}
