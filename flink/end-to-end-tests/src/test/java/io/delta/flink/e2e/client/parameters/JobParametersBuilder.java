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

package io.delta.flink.e2e.client.parameters;

import java.util.HashMap;
import java.util.Map;

/**
 * A builder class for {@link JobParameters}.
 * <p>
 * After instantiation of this builder you can either call {@link JobParametersBuilder#build()}
 * method to get the instance of a {@link JobParameters} or configure additional options using
 * builder's API.
 */
public final class JobParametersBuilder {
    private String jarId;
    private String entryPointClassName;
    private int parallelism;
    private final Map<String, String> arguments = new HashMap<>();

    private JobParametersBuilder() {
    }

    /**
     * @return a new {@link JobParametersBuilder}
     */
    public static JobParametersBuilder builder() {
        return new JobParametersBuilder();
    }

    /**
     * @param jarId String value that identifies a jar. When uploading the jar a path is returned,
     *              where the filename is the ID. This value is equivalent to the `id` field in the
     *              list of uploaded jars (/jars).
     * @return builder for {@link JobParameters}
     */
    public JobParametersBuilder withJarId(String jarId) {
        this.jarId = jarId;
        return this;
    }

    /**
     * @param entryPointClassName fully qualified name of the entry point class
     * @return builder for {@link JobParameters}
     */
    public JobParametersBuilder withEntryPointClassName(String entryPointClassName) {
        this.entryPointClassName = entryPointClassName;
        return this;
    }

    /**
     * @param parallelism positive integer value that specifies the desired parallelism for the job
     * @return builder for {@link JobParameters}
     */
    public JobParametersBuilder withParallelism(int parallelism) {
        this.parallelism = parallelism;
        return this;
    }

    /**
     * @param key   String value that specifies the key of the job argument
     * @param value String value that specifies the value of the job argument
     * @return builder for {@link JobParameters}
     */
    public JobParametersBuilder withArgument(String key, Object value) {
        this.arguments.put(key, value.toString());
        return this;
    }

    /**
     * Adds an argument containing the name of the test. Note that the flink job should set this
     * name when triggering the program execution.
     *
     * @param jobName desired name of the job
     * @return builder for {@link JobParameters}
     */
    public JobParametersBuilder withName(String jobName) {
        return withArgument("test-name", String.format("\"%s\"", jobName));
    }

    /**
     * Adds an argument containing the test data location
     *
     * @param deltaTablePath path to Delta table
     * @return builder for {@link JobParameters}
     */
    public JobParametersBuilder withDeltaTablePath(String deltaTablePath) {
        return withArgument("delta-table-path", deltaTablePath);
    }

    /**
     * Adds an argument containing information whether the Delta table is partitioned.
     *
     * @param isPartitioned whether the Delta table is partitioned
     * @return builder for {@link JobParameters}
     */
    public JobParametersBuilder withTablePartitioned(boolean isPartitioned) {
        return withArgument("is-table-partitioned", isPartitioned);
    }

    /**
     * Adds an argument containing information on how many records the table with the test data
     * originally contained.
     *
     * @param inputRecords number of records in the test table
     * @return builder for {@link JobParameters}
     */
    public JobParametersBuilder withInputRecords(int inputRecords) {
        return withArgument("input-records", inputRecords);
    }

    /**
     * Adds an argument containing information that a failover should be triggered during the test.
     *
     * @param triggerFailover whether failover should be triggered
     * @return builder for {@link JobParameters}
     */
    public JobParametersBuilder withTriggerFailover(boolean triggerFailover) {
        return withArgument("trigger-failover", triggerFailover);
    }

    /**
     * Creates an instance of {@link JobParameters}.
     *
     * @return constructed {@link JobParameters} instance
     */
    public JobParameters build() {
        return new JobParameters(jarId, entryPointClassName, parallelism, arguments);
    }
}
