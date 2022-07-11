package io.delta.flink.e2e.datagenerator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.data.util.DataFormatConverters.DataFormatConverter;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

public class TestDataGenerator implements Serializable {

    private static final List<String> SAMPLE_COUNTRIES = new ArrayList<>();

    static {
        SAMPLE_COUNTRIES.add("US");
        SAMPLE_COUNTRIES.add("GB");
        SAMPLE_COUNTRIES.add("PL");
        SAMPLE_COUNTRIES.add("CN");
    }

    public static final DataFormatConverter<RowData, Row> CONVERTER =
        DataFormatConverters.getConverterForDataType(
            TypeConversions.fromLogicalToDataType(TestRowType.ROW_TYPE)
        );

    public RowData get(int v) {
        return CONVERTER.toInternal(
            Row.of(
                String.valueOf(v),
                String.valueOf((v + v)),
                getRandomCountry(),
                getRandomYear()
            )
        );
    }

    public int getRandomYear() {
        return ThreadLocalRandom.current().nextInt(1971, 1975);
    }

    public String getRandomCountry() {
        return SAMPLE_COUNTRIES
            .get(ThreadLocalRandom.current().nextInt(0, SAMPLE_COUNTRIES.size()));
    }
}
