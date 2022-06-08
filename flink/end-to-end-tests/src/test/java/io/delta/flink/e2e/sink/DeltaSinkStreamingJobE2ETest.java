package io.delta.flink.e2e.sink;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableSet;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static io.delta.flink.e2e.assertions.DeltaLogAssertions.assertThat;
import static io.delta.flink.e2e.sink.DeltaSinkStreamingJobParameters.DeltaSinkStreamingJobParametersBuilder.deltaSinkStreamingJobParameters;
import static io.delta.flink.e2e.utils.AwsUtils.uploadDirectoryToS3;

import io.delta.standalone.DeltaLog;

class DeltaSinkStreamingJobE2ETest {

    JobClient jobClient;

    @BeforeEach
    void setUp() throws InterruptedException {
        uploadTestData();
    }

    private void uploadTestData() throws InterruptedException {
        uploadDirectoryToS3(
            "databricks-performance-benchmarks-data",
            "e2e-test-data",
            "/home/grzegorz/Projects/grzegorz8/connectors/flink/end-to-end-tests/test-data/"
        );
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

    @Test
    void shouldAddNewRecords() throws Exception {
        // GIVEN
        String tablePathS3a = "s3a://databricks-performance-benchmarks-data/e2e-test-data/" +
            "test-non-partitioned-delta-table-initial-state/";
        // AND
        DeltaLog deltaLog = DeltaLog.forTable(DeltaSinkTestUtils.getHadoopConf(), tablePathS3a);
        long initialDeltaVersion = deltaLog.snapshot().getVersion();
        // AND
        DeltaSinkStreamingJobParameters parameters = deltaSinkStreamingJobParameters()
            .withDeltaTablePath(tablePathS3a)
            .withIsPartitioned(false)
            .withSourceParallelism(3)
            .withSinkParallelism(3)
            .withInputRecordsCount(10_000)
            .build();

        // WHEN
        StreamExecutionEnvironment env = getExecutionEnvironment();
        DeltaSinkStreamingJob.getPipeline(env, parameters);
        jobClient = env.executeAsync("[E2E] Sink: add new records test");

        Thread.sleep(2 * 60 * 1000L);

        // THEN
        assertThat(deltaLog)
            .sinceVersion(initialDeltaVersion)
            .hasNewRecordsCount(10_000);
    }

    private boolean jobSucceeded() throws Exception {
        JobStatus jobStatus = jobClient.getJobStatus().get(5, TimeUnit.SECONDS);
        if (UNSUCCESSFUL_JOB_STATUSES.contains(jobStatus)) {
            Assertions.fail(
                String.format("Job has failed or has been cancelled; status=%s.", jobStatus)
            );
        }
        return JOB_SUCCEEDED_STATUSES.contains(jobStatus);
    }

    private static final Set<JobStatus> JOB_IN_PROGRESS_STATUSES =
        ImmutableSet.<JobStatus>builder()
            .add(JobStatus.INITIALIZING)
            .add(JobStatus.RUNNING)
            .add(JobStatus.RECONCILING)
            .add(JobStatus.RESTARTING)
            .add(JobStatus.SUSPENDED)
            .build();

    private static final Set<JobStatus> JOB_SUCCEEDED_STATUSES =
        ImmutableSet.<JobStatus>builder()
            .add(JobStatus.FINISHED)
            .build();

    private static final Set<JobStatus> UNSUCCESSFUL_JOB_STATUSES =
        ImmutableSet.<JobStatus>builder()
            .add(JobStatus.CANCELED)
            .add(JobStatus.CANCELLING)
            .add(JobStatus.FAILED)
            .add(JobStatus.FAILING)
            .build();

    private StreamExecutionEnvironment getExecutionEnvironment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(
            "localhost",
            8081,
            "/home/grzegorz/Projects/grzegorz8/connectors/flink/end-to-end-tests/target/" +
                "scala-2.12/delta-flink-end-to-end-tests-assembly-0.4.0-SNAPSHOT.jar"
        );
        return env;
    }

}
