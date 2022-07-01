package io.delta.flink.e2e.sink;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;

class RowDataListGenerator {

    static List<RowData> getTestRowData(int recordCount) {
        List<RowData> rows = new ArrayList<>(recordCount);
        for (int i = 0; i < recordCount; i++) {
            Integer v = i;
            rows.add(
                TestRowTypes.CONVERTER.toInternal(
                    Row.of(
                        String.valueOf(v),
                        String.valueOf((v + v)),
                        v)
                )
            );
        }
        return rows;
    }

}
