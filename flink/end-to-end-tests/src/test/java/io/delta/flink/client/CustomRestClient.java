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

package io.delta.flink.client;

import java.util.stream.Collectors;

import io.delta.flink.client.parameters.JobParameters;
import okhttp3.HttpUrl;
import okhttp3.Request;
import okhttp3.RequestBody;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CustomRestClient extends RestClientBase implements FlinkClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(CustomRestClient.class);

    private String jobId;

    CustomRestClient(String host, int port) {
        super(host, port);
    }

    @Override
    public void run(JobParameters parameters) throws Exception {
        String programArgumentsString = parameters.getArguments().entrySet().stream()
            .map(e -> String.format("--%s %s", e.getKey(), e.getValue()))
            .collect(Collectors.joining(" "));

        HttpUrl url = new HttpUrl.Builder()
            .scheme("http")
            .host(host)
            .port(port)
            .addPathSegment("jars")
            .addPathSegment(parameters.getJarId())
            .addPathSegment("run")
            .addQueryParameter("entry-class", parameters.getEntryPointClassName())
            .addQueryParameter("parallelism", Integer.toString(parameters.getParallelism()))
            .addQueryParameter("program-args", programArgumentsString)
            .build();

        Request request = new Request.Builder()
            .post(RequestBody.create(new byte[0]))
            .url(url)
            .build();

        try {
            LOGGER.info("Submitting flink job; parameters: {}", parameters);
            RunJarResponse runJarResponse = executeRequest(request, RunJarResponse.class);
            jobId = runJarResponse.getJobid();
            LOGGER.info("Job submitted; jobId={}.", jobId);
        } catch (Exception e) {
            LOGGER.error("Failed to submit job.", e);
            throw e;
        }
    }

    @Override
    public void cancel() throws Exception {
        HttpUrl url = new HttpUrl.Builder()
            .scheme("http")
            .host(host)
            .port(port)
            .addPathSegment("jobs")
            .addPathSegment(jobId)
            .build();
        Request request = new Request.Builder()
            .patch(RequestBody.create(new byte[0]))
            .url(url)
            .build();
        executeRequest(request);
    }

    @Override
    public JobStatus getStatus() throws Exception {
        HttpUrl url = new HttpUrl.Builder()
            .scheme("http")
            .host(host)
            .port(port)
            .addPathSegment("jobs")
            .addPathSegment(jobId)
            .build();
        Request request = new Request.Builder().get().url(url).build();
        JobStatusResponse statusResponse = executeRequest(request, JobStatusResponse.class);
        return JobStatus.valueOf(statusResponse.getState());
    }

    @Override
    public JobID getJobId() {
        return JobID.fromHexString(jobId);
    }

    private static class RunJarResponse {
        private String jobid;

        public String getJobid() {
            return jobid;
        }

        public void setJobid(String jobid) {
            this.jobid = jobid;
        }

        @Override
        public String toString() {
            return "RunJarResponse{jobid='" + jobid + "'}";
        }
    }

    private static class JobStatusResponse {

        private String state;

        public String getState() {
            return state;
        }

        public void setState(String state) {
            this.state = state;
        }

        @Override
        public String toString() {
            return "JobStatusResponse{" +
                "state='" + state + '\'' +
                '}';
        }
    }
}
