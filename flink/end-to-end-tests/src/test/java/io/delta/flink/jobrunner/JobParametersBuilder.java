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

package io.delta.flink.jobrunner;

import java.util.HashMap;
import java.util.Map;

public final class JobParametersBuilder {
    private String jarPath;
    private String entryPointClassName;
    private int parallelism;
    private final Map<String, String> arguments = new HashMap<>();

    private JobParametersBuilder() {
    }

    public static JobParametersBuilder builder() {
        return new JobParametersBuilder();
    }

    public JobParametersBuilder withJarPath(String jarPath) {
        this.jarPath = jarPath;
        return this;
    }

    public JobParametersBuilder withEntryPointClassName(String entryPointClassName) {
        this.entryPointClassName = entryPointClassName;
        return this;
    }


    public JobParametersBuilder withParallelism(int parallelism) {
        this.parallelism = parallelism;
        return this;
    }

    public JobParametersBuilder withArgument(String key, Object value) {
        this.arguments.put(key, value.toString());
        return this;
    }

    public JobParametersBuilder withName(String jobName) {
        this.arguments.put("test-name", jobName);
        return this;
    }

    public JobParameters build() {
        return new JobParameters(jarPath, entryPointClassName, parallelism, arguments);
    }
}
