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
