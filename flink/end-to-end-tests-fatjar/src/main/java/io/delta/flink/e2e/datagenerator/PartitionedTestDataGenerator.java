package io.delta.flink.e2e.datagenerator;

import java.util.concurrent.ThreadLocalRandom;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.data.util.DataFormatConverters.DataFormatConverter;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

public class PartitionedTestDataGenerator implements TestDataGenerator {
    @Override
    public RowData get(int v) {
        return TEST_PARTITIONED_ROW_CONVERTER.toInternal(
            Row.of(
                String.valueOf(v),
                String.valueOf((v + v)),
                v,
                Integer.toString(ThreadLocalRandom.current().nextInt(0, 2)),
                Integer.toString(ThreadLocalRandom.current().nextInt(0, 2))
            )
        );
    }

    public static final DataFormatConverter<RowData, Row> TEST_PARTITIONED_ROW_CONVERTER =
        DataFormatConverters.getConverterForDataType(
            TypeConversions.fromLogicalToDataType(TestRowTypes.TEST_PARTITIONED_ROW_TYPE)
        );
}
