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
import java.time.Instant;
import java.util.UUID;

import io.delta.flink.client.FlinkClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static io.delta.flink.utils.AwsUtils.removeS3DirectoryRecursively;
import static io.delta.flink.utils.AwsUtils.uploadDirectoryToS3;
import static org.junit.jupiter.api.Assertions.assertNotNull;

abstract class DeltaSinkJobEndToEndTestBase {

    protected static final Logger LOGGER =
        LoggerFactory.getLogger(DeltaSinkJobEndToEndTestBase.class);

    protected FlinkClient flinkClient;
    protected String bucketName;
    protected String testDataLocationPrefix;
    protected String deltaTableLocation;

    @BeforeEach
    void setUp() throws InterruptedException {
        bucketName = getTestS3BucketName();
        uploadTestData();
        flinkClient = getFlinkJobClient();
    }

    protected static String getTestArtifactPath() {
        String jarPath = System.getProperty("E2E_JAR_PATH");
        assertNotNull(jarPath, "Artifact path has not been specified.");
        return jarPath;
    }

    protected static String getTestS3BucketName() {
        String s3BucketName = System.getProperty("E2E_S3_BUCKET_NAME");
        assertNotNull(s3BucketName, "S3 bucket name has not been specified.");
        return s3BucketName;
    }

    protected static String getTestDataLocalPath() {
        String testDataLocalPath = System.getProperty("E2E_TEST_DATA_LOCAL_PATH");
        assertNotNull(testDataLocalPath, "Test data local path has not been specified.");
        return testDataLocalPath;
    }

    private void uploadTestData() throws InterruptedException {
        testDataLocationPrefix = "flink-connector-e2e-tests/" + UUID.randomUUID();
        deltaTableLocation = String.format("s3a://%s/%s/", bucketName, testDataLocationPrefix);
        LOGGER.info("Uploading test data to S3 {}", deltaTableLocation);
        uploadDirectoryToS3(bucketName, testDataLocationPrefix, getTestDataLocalPath());
        LOGGER.info("Test data uploaded.");
    }

    protected static String getJobManagerHost() {
        String jobmanagerHost = System.getProperty("E2E_JOBMANAGER_HOSTNAME");
        assertNotNull(jobmanagerHost, "Flink JobManager hostname has not been specified.");
        return jobmanagerHost;
    }

    protected static int getJobManagerPort() {
        String jobmanagerPortString = System.getProperty("E2E_JOBMANAGER_PORT");
        assertNotNull(jobmanagerPortString, "Flink JobManager port has not been specified.");
        return Integer.parseInt(jobmanagerPortString);
    }

    protected abstract FlinkClient getFlinkJobClient();

    @AfterEach
    void cleanUp() throws Exception {
        cancelJobIfRunning();
        removeTestDataIfNeeded();
    }

    private void cancelJobIfRunning() throws Exception {
        if (flinkClient != null && !flinkClient.isFinished()) {
            LOGGER.warn("Cancelling job {}.", flinkClient.getJobId());
            flinkClient.cancel();
            LOGGER.warn("Job cancelled.");
        }
    }

    private void removeTestDataIfNeeded() {
        String preserveS3Data = System.getProperty("E2E_PRESERVE_S3_DATA");
        if (!"yes".equalsIgnoreCase(preserveS3Data)) {
            LOGGER.info("Removing test data in S3 {}", deltaTableLocation);
            removeS3DirectoryRecursively(bucketName, testDataLocationPrefix);
            LOGGER.info("Test data removed.");
        } else {
            LOGGER.info("Skip test data removal.");
        }
    }

    protected void wait(Duration waitTime) throws Exception {
        Instant waitUntil = Instant.now().plus(waitTime);
        while (!flinkClient.isFinished() && Instant.now().isBefore(waitUntil)) {
            if (flinkClient.isFailed() || flinkClient.isCanceled()) {
                Assertions.fail(
                    String.format("Job has failed or has been cancelled; status=%s.",
                        flinkClient.getStatus())
                );
            }
            Thread.sleep(5_000L);
            LOGGER.warn("Waiting until {}", waitUntil);
        }
    }

    protected String getPartitionedTablePath() {
        return deltaTableLocation + "test-partitioned-delta-table-initial-state";
    }

    protected String getNonPartitionedTablePath() {
        return deltaTableLocation + "test-non-partitioned-delta-table-initial-state";
    }

}
