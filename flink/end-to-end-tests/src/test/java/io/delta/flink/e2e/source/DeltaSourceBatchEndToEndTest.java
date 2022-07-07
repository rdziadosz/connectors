package io.delta.flink.e2e.source;

import java.time.Duration;

import io.delta.flink.e2e.client.parameters.JobParameters;
import io.delta.flink.e2e.client.parameters.JobParametersBuilder;
import io.delta.flink.e2e.data.UserDeltaTable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import static io.delta.flink.e2e.assertions.ParquetAssertions.assertThat;
import static io.delta.flink.e2e.data.UserBuilder.anUser;

@DisplayNameGeneration(DisplayNameGenerator.IndicativeSentences.class)
public class DeltaSourceBatchEndToEndTest extends DeltaSourceEndToEndTestBase {

    private static final int PARALLELISM = 3;
    private static final String JOB_MAIN_CLASS = "io.delta.flink.e2e.source.DeltaSourceBatchJob";


    @DisplayName("Connector in batch mode should read records from the Delta Table")
    @ParameterizedTest(name = "partitioned table: {0}; failover: {1}")
    @CsvSource(value = {"false,false", "true,false"})
    void shouldReadLatestSnapshot(boolean triggerFailover, boolean isPartitioned) throws Exception {
        // GIVEN: Delta Lake table
        UserDeltaTable userTable = isPartitioned
            ? UserDeltaTable.partitionedByCountryAndBirthYear(deltaTableLocation)
            : UserDeltaTable.nonPartitioned(deltaTableLocation);
        userTable.initializeTable();
        // AND: version 1
        userTable.add(
            anUser().name("Philip").surname("Dick").country("US").birthYear(1928).build(),
            anUser().name("Stanisław").surname("Lem").country("PL").birthYear(1921).build()
        );
        // AND: version 2
        userTable.add(
            anUser().name("Arthur").surname("Clarke").country("US").birthYear(1917).build(),
            anUser().name("Isaac").surname("Asimov").country("US").birthYear(1920).build()
        );
        // AND
        JobParameters jobParameters = batchJobParameters()
            .withDeltaTablePath(deltaTableLocation)
            .withTablePartitioned(isPartitioned)
            .withTriggerFailover(triggerFailover)
            .build();

        // WHEN
        jobID = flinkClient.run(jobParameters);
        wait(Duration.ofMinutes(3));

        // THEN
        assertThat(outputLocation)
            .hasRecordCount(4);
    }

    private JobParametersBuilder batchJobParameters() {
        return JobParametersBuilder.builder()
            .withName(getTestDisplayName())
            .withJarId(jarId)
            .withEntryPointClassName(JOB_MAIN_CLASS)
            .withParallelism(PARALLELISM)
            .withOutputPath(outputLocation);
    }

}
