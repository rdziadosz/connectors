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

package io.delta.flink.e2e.assertions;

import java.io.IOException;
import java.util.List;

import io.delta.flink.e2e.parquet.AvroParquetFileReader;
import io.delta.flink.e2e.utils.HadoopConfig;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Assertions;

public class ParquetAssertions {

    public static ParquetAsserter assertThat(String baseFilePath) {
        return new ParquetAsserter(baseFilePath);
    }

    public static class ParquetAsserter {
        private String baseFilePath;

        public ParquetAsserter(String baseFilePath) {
            this.baseFilePath = baseFilePath;
        }

        public ParquetAsserter hasRecordCount(int expectedCount) throws IOException {
            AvroParquetFileReader reader =
                new AvroParquetFileReader(HadoopConfig.get());
            List<GenericRecord> actualRecords = reader.readRecursively(baseFilePath);
            Assertions.assertEquals(expectedCount, actualRecords.size());
            return this;
        }
    }

}
