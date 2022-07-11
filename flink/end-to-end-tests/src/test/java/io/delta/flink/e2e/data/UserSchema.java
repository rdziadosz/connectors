package io.delta.flink.e2e.data;

import java.util.Arrays;

import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import io.delta.standalone.types.IntegerType;
import io.delta.standalone.types.StringType;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;

public class UserSchema {

    public static final StructType STRUCT_TYPE = new StructType()
        .add(new StructField("name", new StringType(), true))
        .add(new StructField("surname", new StringType(), true))
        .add(new StructField("country", new StringType(), true))
        .add(new StructField("birthYear", new IntegerType(), true));

    public static final RowType ROW_TYPE = new RowType(Arrays.asList(
        new RowType.RowField("name", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("surname", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("country", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("birthYear", new IntType())
    ));

}
