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

import java.io.File;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.messages.Acknowledge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class FlinkJobRestClient implements FlinkJobClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkJobRestClient.class);

    private final String host;
    private final int port;
    private JobID jobId;

    FlinkJobRestClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    private Configuration getConfiguration() {
        Configuration config = new Configuration();
        config.setString(JobManagerOptions.ADDRESS, host);
        config.setInteger(RestOptions.PORT, port);
        return config;
    }

    @Override
    public void run(JobParameters parameters) throws Exception {
        Configuration config = getConfiguration();
        String[] args = getArguments(parameters.getArguments());

        PackagedProgram packagedProgram = PackagedProgram.newBuilder()
            .setJarFile(new File(parameters.getJarPath()))
            .setEntryPointClassName(parameters.getEntryPointClassName())
            .setConfiguration(config)
            .setArguments(args)
            .build();

        JobGraph jobGraph = PackagedProgramUtils.createJobGraph(
            packagedProgram,
            config,
            parameters.getParallelism(),
            false
        );

        try (RestClusterClient<StandaloneClusterId> client =
                 new RestClusterClient<>(config, StandaloneClusterId.getInstance())) {
            LOGGER.info("Submitting flink job; parameters: {}", parameters);
            jobId = client.submitJob(jobGraph).get(60, TimeUnit.SECONDS);
            LOGGER.info("Job submitted; jobId={}.", jobId);
        }
    }

    private String[] getArguments(Map<String, String> arguments) {
        return arguments.entrySet().stream()
            .flatMap(entry -> Stream.of(String.format("--%s", entry.getKey()), entry.getValue()))
            .toArray(String[]::new);
    }

    @Override
    public Acknowledge cancel() throws Exception {
        Configuration config = getConfiguration();
        try (RestClusterClient<StandaloneClusterId> client =
                 new RestClusterClient<>(config, StandaloneClusterId.getInstance())) {
            return client.cancel(jobId).get(60, TimeUnit.SECONDS);
        }
    }

    @Override
    public JobStatus getStatus() throws Exception {
        Configuration config = getConfiguration();
        try (RestClusterClient<StandaloneClusterId> client =
                 new RestClusterClient<>(config, StandaloneClusterId.getInstance())) {
            return client.getJobStatus(jobId).get(15, TimeUnit.SECONDS);
        }
    }

    @Override
    public boolean isFinished() throws Exception {
        return getStatus().equals(JobStatus.FINISHED);
    }

    @Override
    public boolean isFailed() throws Exception {
        return getStatus().equals(JobStatus.FAILED);
    }

    @Override
    public boolean isCanceled() throws Exception {
        return getStatus().equals(JobStatus.CANCELED);
    }

    @Override
    public JobID getJobId() {
        return jobId;
    }

}
