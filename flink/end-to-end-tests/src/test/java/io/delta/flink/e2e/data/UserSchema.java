/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
