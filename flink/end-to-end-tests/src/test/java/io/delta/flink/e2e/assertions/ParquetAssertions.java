package io.delta.flink.e2e.assertions;

import java.io.IOException;
import java.util.List;

import io.delta.flink.e2e.parquet.AvroParquetFileReader;
import io.delta.flink.e2e.utils.HadoopConfig;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Assertions;

public class ParquetAssertions {

    public static ParquetAsserter assertThat(String baseFilePath) {
        return new ParquetAsserter(baseFilePath);
    }

    public static class ParquetAsserter {
        private String baseFilePath;

        public ParquetAsserter(String baseFilePath) {
            this.baseFilePath = baseFilePath;
        }

        public ParquetAsserter hasRecordCount(int expectedCount) throws IOException {
            AvroParquetFileReader reader =
                new AvroParquetFileReader(HadoopConfig.get());
            List<GenericRecord> actualRecords = reader.readRecursively(baseFilePath);
            Assertions.assertEquals(expectedCount, actualRecords.size());
            return this;
        }
    }

}
