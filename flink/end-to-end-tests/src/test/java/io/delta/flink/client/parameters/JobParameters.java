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

package io.delta.flink.client.parameters;

import java.util.Map;

public class JobParameters {

    private final String jarPath;
    private final String jarId;
    private final String entryPointClassName;
    private final int parallelism;
    private final Map<String, String> arguments;

    JobParameters(String jarPath,
                  String jarId, String entryPointClassName,
                  int parallelism,
                  Map<String, String> arguments) {
        this.jarPath = jarPath;
        this.jarId = jarId;
        this.entryPointClassName = entryPointClassName;
        this.parallelism = parallelism;
        this.arguments = arguments;
    }

    public String getJarPath() {
        return jarPath;
    }

    public String getJarId() {
        return jarId;
    }

    public String getEntryPointClassName() {
        return entryPointClassName;
    }

    public int getParallelism() {
        return parallelism;
    }

    public Map<String, String> getArguments() {
        return arguments;
    }

    @Override
    public String toString() {
        return "JobParameters{" +
            "jarPath='" + jarPath + '\'' +
            ", jarId='" + jarId + '\'' +
            ", entryPointClassName='" + entryPointClassName + '\'' +
            ", parallelism=" + parallelism +
            ", arguments=" + arguments +
            '}';
    }
}
