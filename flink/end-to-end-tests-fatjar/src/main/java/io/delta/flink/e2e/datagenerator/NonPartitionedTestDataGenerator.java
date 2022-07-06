package io.delta.flink.e2e.datagenerator;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.data.util.DataFormatConverters.DataFormatConverter;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

public class NonPartitionedTestDataGenerator implements TestDataGenerator {
    @Override
    public RowData get(int v) {
        return TEST_ROW_CONVERTER.toInternal(
            Row.of(
                String.valueOf(v),
                String.valueOf((v + v)),
                v
            )
        );
    }

    public static final DataFormatConverter<RowData, Row> TEST_ROW_CONVERTER =
        DataFormatConverters.getConverterForDataType(
            TypeConversions.fromLogicalToDataType(TestRowTypes.TEST_ROW_TYPE)
        );
}
