package io.delta.flink.e2e.sink;

import java.time.Duration;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;
import static io.delta.flink.e2e.assertions.DeltaLogAssertions.assertThat;
import static io.delta.flink.e2e.sink.DeltaSinkStreamingJobParameters.DeltaSinkStreamingJobParametersBuilder.deltaSinkStreamingJobParameters;

import io.delta.standalone.DeltaLog;

class DeltaSinkStreamingJobE2ETest extends DeltaSinkStreamingJobE2EBase {

    @Test
    void shouldAddNewRecords() throws Exception {
        // GIVEN
        String tablePathS3 = getNonPartitionedTableInitialStateLocation();
        // AND
        DeltaLog deltaLog = DeltaLog.forTable(DeltaSinkTestUtils.getHadoopConf(), tablePathS3);
        long initialDeltaVersion = deltaLog.snapshot().getVersion();
        // AND
        DeltaSinkStreamingJobParameters params = deltaSinkStreamingJobParameters()
            .withDeltaTablePath(tablePathS3)
            .withIsPartitioned(false)
            .withSourceParallelism(3)
            .withSinkParallelism(3)
            .withInputRecordsCount(10_000)
            .build();

        // WHEN
        StreamExecutionEnvironment env = getExecutionEnvironment();
        DeltaSinkStreamingJob.getPipeline(env, params);
        jobClient = env.executeAsync("[E2E] Sink: add new records test");
        wait(Duration.ofSeconds(120L));

        // THEN
        assertThat(deltaLog)
            .sinceVersion(initialDeltaVersion)
            .hasNewRecordsCount(params.getInputRecordsCount() * params.getSourceParallelism());
    }

}
