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

package io.delta.flink.e2e.assertions;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.LongStream;

import io.delta.flink.utils.TestParquetReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.standalone.DeltaLog;

public class DeltaLogAssertions {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeltaLogAssertions.class);

    public static DeltaLogAsserter assertThat(DeltaLog deltaLog) {
        LOGGER.info("Verifying Delta Lake table.");
        return new DeltaLogAsserter(deltaLog);
    }

    public static class DeltaLogAsserter {

        private final DeltaLog deltaLog;
        private Long referenceVersion;

        public DeltaLogAsserter(DeltaLog deltaLog) {
            this.deltaLog = deltaLog;
        }

        public DeltaLogAsserter sinceVersion(long referenceVersion) {
            this.referenceVersion = referenceVersion;
            return this;
        }

        public DeltaLogAsserter hasNewRecordCountInOperationMetrics(int expectedNewRecordsCount) {
            checkPrerequisites();
            long currentVersion = deltaLog.update().getVersion();
            long actualNewRecordsCount = getNewVersionsStream(currentVersion)
                .map(this::getNumOutputRowsForVersion)
                .sum();
            assertEquals(expectedNewRecordsCount, actualNewRecordsCount);
            return this;
        }

        public DeltaLogAsserter metricsNewFileCountMatchesSnapshotNewFileCount() {
            checkPrerequisites();
            long currentVersion = deltaLog.update().getVersion();

            int initialFileCount = deltaLog.getSnapshotForVersionAsOf(referenceVersion)
                .getAllFiles().size();
            int currentFileCount = deltaLog.update().getAllFiles().size();
            int snapshotNewFileCount = currentFileCount - initialFileCount;

            long newFilesCountInOperationMetrics = getNewVersionsStream(currentVersion)
                .map(this::getNumAddedFilesForVersion)
                .sum();

            assertEquals(snapshotNewFileCount, newFilesCountInOperationMetrics);
            return this;
        }

        public DeltaLogAsserter hasPositiveNumOutputBytesInEachVersion() {
            checkPrerequisites();
            long currentVersion = deltaLog.update().getVersion();
            getNewVersionsStream(currentVersion)
                .forEach(version -> {
                    long bytes = getNumOutputBytesForVersion(version);
                    assertTrue(bytes > 0, "Bytes count is not positive in version " + version);
                });
            return this;
        }

        public DeltaLogAsserter hasRecordCountInParquetFiles(int expectedRecordCount)
            throws IOException {
            int actualRecordCount = TestParquetReader.readAndValidateAllTableRecords(deltaLog);
            assertEquals(expectedRecordCount, actualRecordCount);
            return this;
        }

        private LongStream getNewVersionsStream(long currentVersion) {
            return LongStream.range(referenceVersion + 1, currentVersion + 1);
        }

        private long getNumOutputRowsForVersion(long version) {
            Map<String, String> currentMetrics = getOperationMetricsForVersion(version);
            return Long.parseLong(currentMetrics.get("numOutputRows"));
        }

        private long getNumAddedFilesForVersion(long version) {
            Map<String, String> currentMetrics = getOperationMetricsForVersion(version);
            return Long.parseLong(currentMetrics.get("numAddedFiles"));
        }

        private long getNumOutputBytesForVersion(long version) {
            Map<String, String> currentMetrics = getOperationMetricsForVersion(version);
            return Long.parseLong(currentMetrics.get("numOutputBytes"));
        }

        private Map<String, String> getOperationMetricsForVersion(long version) {
            Optional<Map<String, String>> operationMetrics = deltaLog.getCommitInfoAt(version)
                .getOperationMetrics();
            assertTrue(operationMetrics.isPresent());
            return operationMetrics.get();
        }

        public DeltaLogAsserter hasCheckpointsCount(int expectedCount) throws IOException {
            Path deltaLogPath = new Path(deltaLog.getPath(), "_delta_log");
            Set<String> files = getFileList(deltaLogPath);
            long actualCheckpoints = files.stream()
                .filter(p -> p.endsWith("checkpoint.parquet")).count();
            assertEquals(expectedCount, actualCheckpoints);
            return this;
        }

        public DeltaLogAsserter hasLastCheckpointFile() throws IOException {
            Path deltaLogPath = new Path(deltaLog.getPath(), "_delta_log");
            Set<String> files = getFileList(deltaLogPath);
            long actualCheckpoints = files.stream()
                .filter(p -> p.endsWith("_last_checkpoint")).count();
            assertEquals(1, actualCheckpoints);
            return this;
        }

        private Set<String> getFileList(Path path) throws IOException {
            FileSystem fileSystem = path.getFileSystem(new Configuration());
            Set<String> files = new HashSet<>();
            RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(path, true);
            while (iterator.hasNext()) {
                LocatedFileStatus file = iterator.next();
                files.add(file.getPath().toString());
            }
            return files;
        }

        private void checkPrerequisites() {
            assertNotNull(referenceVersion, "Initial Delta Lake version has not been provided.");
        }
    }

}
