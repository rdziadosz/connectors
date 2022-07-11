package io.delta.flink.e2e.data;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.delta.flink.e2e.utils.HadoopConfig;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.row.ParquetRowDataBuilder;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.data.util.DataFormatConverters.DataFormatConverter;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Operation;
import io.delta.standalone.OptimisticTransaction;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.Metadata;

public class UserDeltaTable {

    private static final Logger LOGGER = LoggerFactory.getLogger(UserDeltaTable.class);

    public static UserDeltaTable nonPartitioned(String tablePath) {
        return new UserDeltaTable(tablePath);
    }


    public static UserDeltaTable partitionedByCountryAndBirthYear(String tablePath) {
        List<Column> partitionColumns = new ArrayList<>(2);
        partitionColumns.add(Column.of("country", User::getCountry));
        partitionColumns.add(Column.of("birthYear", User::getBirthYear));
        return new UserDeltaTable(tablePath, partitionColumns);
    }

    private static class Column {

        static Column of(String name, Function<User, Object> accessor) {
            return new Column(name, accessor);
        }

        private final String name;
        private final Function<User, Object> accessor;

        private Column(String name, Function<User, Object> accessor) {
            this.name = name;
            this.accessor = accessor;
        }

        String getName() {
            return name;
        }

        Function<User, Object> getAccessor() {
            return accessor;
        }
    }

    private static final String ENGINE_INFO = "local";

    private final String tablePath;
    private final Configuration configuration;
    private final DeltaLog deltaLog;
    private final List<Column> partitionColumns;

    private UserDeltaTable(String tablePath) {
        this(tablePath, Collections.emptyList());
    }

    private UserDeltaTable(String tablePath, List<Column> partitionColumns) {
        this.tablePath = tablePath;
        this.configuration = HadoopConfig.get();
        this.deltaLog = DeltaLog.forTable(configuration, tablePath);
        this.partitionColumns = partitionColumns;
    }

    public String getTablePath() {
        return tablePath;
    }

    public DeltaLog getDeltaLog() {
        return deltaLog;
    }

    public void initializeTable() {
        List<String> partitionColumnNames = partitionColumns.stream()
            .map(Column::getName)
            .collect(Collectors.toList());

        Metadata metadata = Metadata.builder()
            .name("Test user table.")
            .description("End-to-end test data table.")
            .schema(UserSchema.STRUCT_TYPE)
            .partitionColumns(partitionColumnNames)
            .build();
        OptimisticTransaction txn = deltaLog.startTransaction();
        Operation operation = new Operation(Operation.Name.CREATE_TABLE);
        txn.commit(Collections.singleton(metadata), operation, ENGINE_INFO);
    }

    public void add(User... users) throws Exception {
        if (deltaLog.update().getMetadata().getPartitionColumns().isEmpty()) {
            addNonPartitioned(users);
        } else {
            addPartitioned(users);
        }
    }

    private void addNonPartitioned(User[] users) throws Exception {
        List<Row> rows = Arrays.stream(users)
            .map(UserConverter::toRow)
            .collect(Collectors.toList());

        Path pathToParquet = writeToParquet(
            deltaLog.getPath().toString(),
            UserSchema.ROW_TYPE,
            rows
        );

        AddFile addFile = AddFile.builder(
                pathToParquet.getPath(),
                Collections.emptyMap(),
                rows.size(),
                System.currentTimeMillis(),
                true
            )
            .build();

        OptimisticTransaction txn = deltaLog.startTransaction();
        Operation op = new Operation(Operation.Name.WRITE);
        txn.commit(Collections.singletonList(addFile), op, ENGINE_INFO);
    }

    private void addPartitioned(User[] users) throws IOException {
        Map<String, List<User>> partitioned = Arrays.stream(users)
            .collect(Collectors.groupingBy(this::getSubPath));

        List<AddFile> operations = new ArrayList<>(partitioned.size());
        for (Map.Entry<String, List<User>> partition : partitioned.entrySet()) {
            AddFile addFile = writePartition(partition.getKey(), partition.getValue());
            operations.add(addFile);
        }

        OptimisticTransaction txn = deltaLog.startTransaction();
        Operation op = new Operation(Operation.Name.WRITE);
        txn.commit(operations, op, ENGINE_INFO);
    }

    private String getSubPath(User user) {
        return partitionColumns.stream()
            .map(column -> {
                String name = column.getName();
                Object value = column.getAccessor().apply(user);
                return String.format("%s=%s", name, value);
            })
            .collect(Collectors.joining("/"));
    }

    private AddFile writePartition(String subPath, List<User> users) throws IOException {
        String path = deltaLog.getPath().toString() + "/" + subPath;
        List<Row> rows = users.stream().map(UserConverter::toRow).collect(Collectors.toList());
        Map<String, String> partitionValues = partitionColumns.stream()
            .collect(Collectors.toMap(
                Column::getName,
                column -> column.getAccessor().apply(users.get(0)).toString()
            ));

        Path pathToParquet = writeToParquet(path, UserSchema.ROW_TYPE, rows);

        return AddFile.builder(
                pathToParquet.getPath(),
                partitionValues,
                users.size(),
                System.currentTimeMillis(),
                true
            )
            .build();
    }

    /**
     * Writes Rows into Parquet files.
     *
     * @param parentPath Root folder under which a Parquet file should be created.
     * @param rowType    A {@link RowType} describing column types for rows.
     * @param rows       A {@link List} of rows to write into the Parquet file.
     * @return A {@link Path} to created Parquet file.
     * @throws IOException {@link IOException} in case of any IO issue during writing to Parquet
     *                     file.
     */
    private Path writeToParquet(String parentPath, RowType rowType, List<Row> rows)
        throws IOException {
        ParquetWriterFactory<RowData> factory =
            ParquetRowDataBuilder.createWriterFactory(rowType, configuration, false);

        Path path = new Path(parentPath, UUID.randomUUID() + ".snappy.parquet");
        FSDataOutputStream stream = path.getFileSystem().create(path, WriteMode.OVERWRITE);
        BulkWriter<RowData> writer = factory.create(stream);

        DataFormatConverter<RowData, Row> converter = getConverter(rowType);
        for (Row row : rows) {
            writer.addElement(converter.toInternal(row));
        }

        writer.flush();
        writer.finish();
        stream.close();

        return path;
    }

    @SuppressWarnings("unchecked")
    private DataFormatConverter<RowData, Row> getConverter(RowType rowType) {
        return (DataFormatConverter<RowData, Row>) DataFormatConverters.getConverterForDataType(
            TypeConversions.fromLogicalToDataType(rowType));
    }

}
