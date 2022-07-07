package io.delta.flink.e2e.source;

import io.delta.flink.e2e.datagenerator.TestRowType;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.row.ParquetRowDataBuilder;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.table.data.RowData;

class TestFileSinkFactory {

    static FileSink<RowData> get(String outputPath) {
        return FileSink.forBulkFormat(
                new Path(outputPath),
                ParquetRowDataBuilder.createWriterFactory(TestRowType.ROW_TYPE,
                    HadoopConfig.get(), true)
            )
            .withRollingPolicy(OnCheckpointRollingPolicy.build())
            .withBucketAssigner(new SinglePartitionBucketAssigner())
            .withOutputFileConfig(OutputFileConfig.builder().withPartSuffix(".parquet").build())
            .build();
    }

}
