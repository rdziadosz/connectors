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
import org.apache.flink.api.common.JobID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static io.delta.flink.utils.AwsUtils.removeS3DirectoryRecursively;
import static io.delta.flink.utils.AwsUtils.uploadDirectoryToS3;
import static org.junit.jupiter.api.Assertions.assertNotNull;

abstract class DeltaSinkEndToEndTestBase {

    protected static final Logger LOGGER =
        LoggerFactory.getLogger(DeltaSinkEndToEndTestBase.class);

    protected String bucketName;
    protected String testDataLocationPrefix;
    protected String deltaTableLocation;
    protected JobID jobID;

    protected static String getTestArtifactPath() {
        return getEnvProperty("E2E_JAR_PATH");
    }

    protected static String getTestS3BucketName() {
        return getEnvProperty("E2E_S3_BUCKET_NAME");
    }

    protected static String getTestDataLocalPath() {
        return getEnvProperty("E2E_TEST_DATA_LOCAL_PATH");
    }

    protected static String getJobManagerHost() {
        return getEnvProperty("E2E_JOBMANAGER_HOSTNAME");
    }

    protected static int getJobManagerPort() {
        String jobmanagerPortString = getEnvProperty("E2E_JOBMANAGER_PORT");
        return Integer.parseInt(jobmanagerPortString);
    }

    private static String getEnvProperty(String name) {
        String property = System.getProperty(name);
        assertNotNull(property, name + " environment property has not been specified.");
        return property;
    }

    @BeforeEach
    void setUp() throws InterruptedException {
        bucketName = getTestS3BucketName();
        uploadTestData();
    }

    private void uploadTestData() throws InterruptedException {
        testDataLocationPrefix = "flink-connector-e2e-tests/" + UUID.randomUUID();
        deltaTableLocation = String.format("s3a://%s/%s/", bucketName, testDataLocationPrefix);
        LOGGER.info("Uploading test data to S3 {}", deltaTableLocation);
        uploadDirectoryToS3(bucketName, testDataLocationPrefix, getTestDataLocalPath());
        LOGGER.info("Test data uploaded.");
    }

    protected abstract FlinkClient getFlinkClient();

    @AfterEach
    void cleanUp() throws Exception {
        cancelJobIfRunning();
        removeTestDataIfNeeded();
    }

    private void cancelJobIfRunning() throws Exception {
        if (getFlinkClient() != null && jobID != null && !getFlinkClient().isFinished(jobID)) {
            LOGGER.info("Cancelling job {}.", jobID);
            getFlinkClient().cancel(jobID);
            jobID = null;
            LOGGER.info("Job cancelled.");
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
        while (!getFlinkClient().isFinished(jobID) && Instant.now().isBefore(waitUntil)) {
            if (getFlinkClient().isFailed(jobID) || getFlinkClient().isCanceled(jobID)) {
                Assertions.fail(
                    String.format("Job has failed or has been cancelled; status=%s.",
                        getFlinkClient().getStatus(jobID))
                );
            }
            Thread.sleep(5_000L);
            LOGGER.info("Waiting until {}", waitUntil);
        }
        Assertions.assertTrue(getFlinkClient().isFinished(jobID),
            "Job has not finished in a timely manner.");
    }

    protected String getPartitionedTablePath() {
        return deltaTableLocation + "test-partitioned-delta-table-initial-state";
    }

    protected String getNonPartitionedTablePath() {
        return deltaTableLocation + "test-non-partitioned-delta-table-initial-state";
    }

}
