package io.delta.flink.e2e.sink;

import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableSet;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import static io.delta.flink.e2e.utils.AwsUtils.uploadDirectoryToS3;

class DeltaSinkStreamingJobE2EBase {

    protected static final Set<JobStatus> JOB_SUCCEEDED_STATUSES =
        ImmutableSet.<JobStatus>builder()
            .add(JobStatus.FINISHED)
            .build();

    protected static final Set<JobStatus> UNSUCCESSFUL_JOB_STATUSES =
        ImmutableSet.<JobStatus>builder()
            .add(JobStatus.CANCELED)
            .add(JobStatus.CANCELLING)
            .add(JobStatus.FAILED)
            .add(JobStatus.FAILING)
            .build();

    protected JobClient jobClient;
    protected String testDataLocation;

    @BeforeEach
    void setUp() throws InterruptedException {
        uploadTestData();
    }

    private void uploadTestData() throws InterruptedException {
        String bucketName = "databricks-performance-benchmarks-data";
        String prefix = "flink-connector-e2e-tests/" + UUID.randomUUID();
        String localPath = "/home/grzegorz/Projects/grzegorz8/connectors/flink/" +
            "end-to-end-tests/test-data/";
        testDataLocation = String.format("s3a://%s/%s/", bucketName, prefix);
        uploadDirectoryToS3(bucketName, prefix, localPath);
    }

    @AfterEach
    void cleanUp() {
        cancelJobIfRunning();
        removeTestData();
    }

    private void cancelJobIfRunning() {
        if (jobClient != null) {
            jobClient.cancel();
        }
    }

    private void removeTestData() {
    }

    protected String getPartitionedTableInitialStateLocation() {
        return testDataLocation + "test-partitioned-delta-table-initial-state";
    }

    protected String getNonPartitionedTableInitialStateLocation() {
        return testDataLocation + "test-non-partitioned-delta-table-initial-state";
    }

    protected String getNonPartitionedTableWith1100Records() {
        return testDataLocation + "test-non-partitioned-delta-table_1100_records";
    }

    protected StreamExecutionEnvironment getExecutionEnvironment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(
            "localhost",
            8081,
            "/home/grzegorz/Projects/grzegorz8/connectors/flink/end-to-end-tests/target/" +
                "scala-2.12/delta-flink-end-to-end-tests-assembly-0.4.0-SNAPSHOT.jar"
        );
        return env;
    }

    protected void wait(Duration waitTime) throws Exception {
        Instant waitUntil = Instant.now().plus(waitTime);
        while (Instant.now().isBefore(waitUntil)) {
            checkIfJobUnsuccessful();
            Thread.sleep(2_000L);
        }
    }

    protected void checkIfJobUnsuccessful() throws Exception {
        JobStatus jobStatus = jobClient.getJobStatus().get(5, TimeUnit.SECONDS);
        if (UNSUCCESSFUL_JOB_STATUSES.contains(jobStatus)) {
            Assertions.fail(
                String.format("Job has failed or has been cancelled; status=%s.", jobStatus)
            );
        }
    }

    protected boolean jobSucceeded() throws Exception {
        JobStatus jobStatus = jobClient.getJobStatus().get(5, TimeUnit.SECONDS);
        if (UNSUCCESSFUL_JOB_STATUSES.contains(jobStatus)) {
            Assertions.fail(
                String.format("Job has failed or has been cancelled; status=%s.", jobStatus)
            );
        }
        return JOB_SUCCEEDED_STATUSES.contains(jobStatus);
    }

}
