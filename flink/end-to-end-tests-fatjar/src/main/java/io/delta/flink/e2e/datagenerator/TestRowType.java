package io.delta.flink.e2e.datagenerator;

import java.util.Arrays;

import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

public class TestRowType {

    public static final RowType ROW_TYPE = new RowType(Arrays.asList(
        new RowType.RowField("name", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("surname", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("country", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("birthYear", new IntType())
    ));

}
