package io.delta.flink.e2e.parquet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import io.delta.flink.e2e.utils.FileSystemUtils;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;

public class AvroParquetFileReader {

    private final Configuration configuration;

    public AvroParquetFileReader(Configuration configuration) {
        this.configuration = configuration;
    }

    public List<GenericRecord> read(String filePath) throws IOException {
        return read(new Path(filePath));
    }

    public List<GenericRecord> read(Path filePath) throws IOException {
        List<GenericRecord> result = new ArrayList<>();
        HadoopInputFile file = HadoopInputFile.fromPath(filePath, configuration);
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.genericRecordReader(file)) {
            for (GenericRecord record = reader.read(); record != null; record = reader.read()) {
                result.add(record);
            }
        }
        return result;
    }


    public List<GenericRecord> readRecursively(String filePath) throws IOException {
        return FileSystemUtils.listFiles(filePath).parallelStream()
            .filter(path -> path.getName().endsWith(".parquet"))
            .filter(path -> !path.toString().contains("_delta_log"))  // FIXME: make it configurable
            .flatMap(path -> {
                try {
                    return read(path).stream();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            })
            .collect(Collectors.toList());
    }

}
