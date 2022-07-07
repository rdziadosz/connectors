package io.delta.flink.e2e.source;

import java.util.UUID;

import io.delta.flink.e2e.DeltaConnectorEndToEndTestBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import static io.delta.flink.e2e.TestParameters.preserveS3Data;
import static io.delta.flink.e2e.utils.AwsUtils.removeS3DirectoryRecursively;

class DeltaSourceEndToEndTestBase extends DeltaConnectorEndToEndTestBase {

    protected String outputLocationPrefix;
    protected String outputLocation;
    protected FileCounter fileCounter;

    @BeforeEach
    protected void setUp(TestInfo testInfo) throws Exception {
        super.setUp(testInfo);
        createTestOutputLocation();
        fileCounter = new FileCounter(outputLocation);
    }

    private void createTestOutputLocation() {
        outputLocationPrefix = "flink-connector-e2e-tests/outputs/" + UUID.randomUUID();
        outputLocation = String.format("s3a://%s/%s/", bucketName, outputLocationPrefix);
        LOGGER.info("Setting output location {}", outputLocation);
    }

    @AfterEach
    protected void cleanUp() throws Exception {
        super.cleanUp();
        removeTestOutputIfNeeded();
    }

    private void removeTestOutputIfNeeded() {
        if (!preserveS3Data()) {
            LOGGER.info("Removing test output in S3 {}", outputLocation);
            removeS3DirectoryRecursively(bucketName, outputLocationPrefix);
            LOGGER.info("Test output removed.");
        } else {
            LOGGER.info("Skip test output removal.");
        }
    }


}
