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

package io.delta.flink.e2e.client;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.stream.Collectors;

import io.delta.flink.e2e.client.parameters.JobParameters;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A custom REST API client which supports a subset of Flink REST API resources.
 * {@link CustomRestClient} has been implemented since Flink batch jobs run by
 * {@link org.apache.flink.client.program.rest.RestClusterClient} do not work properly, namely,
 * checkpoint is not triggered at the end of the job. In consequence, Delta connector does not
 * commit new files. In addition, jar upload and job triggering are two separate methods.
 */
class CustomRestClient implements FlinkClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(CustomRestClient.class);

    private static final ObjectMapper objectMapper;

    static {
        objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private final String host;
    private final int port;
    private final OkHttpClient httpClient;

    CustomRestClient(String host, int port) {
        this.host = host;
        this.port = port;
        this.httpClient = new OkHttpClient.Builder()
            .connectTimeout(Duration.ofSeconds(3))
            .retryOnConnectionFailure(true)
            .build();
    }

    @Override
    public String uploadJar(String jarPath) throws IOException {
        LOGGER.info("Uploading jar: {}", jarPath);
        HttpUrl url = urlBuilder()
            .addPathSegment("jars")
            .addPathSegment("upload")
            .build();

        File jarFile = new File(jarPath);
        RequestBody requestBody = new MultipartBody.Builder()
            .setType(MultipartBody.FORM)
            .addFormDataPart("file", jarFile.getName(),
                RequestBody.create(
                    MediaType.parse("application/x-java-archive"),
                    new File(jarPath))
            )
            .build();

        Request request = new Request.Builder().url(url).post(requestBody).build();
        JarsUploadResponse uploadResponse = executeRequest(request, JarsUploadResponse.class);
        String jarId = substringAfterLastSlash(uploadResponse.getFilename());
        LOGGER.info("Jar uploaded; id={}", jarId);
        return jarId;
    }

    private String substringAfterLastSlash(String filename) {
        String[] parts = filename.split("/");
        return parts[parts.length - 1];
    }

    @Override
    public void deleteJar(String jarId) throws Exception {
        LOGGER.info("Deleting jar {}.", jarId);
        HttpUrl url = urlBuilder()
            .addPathSegment("jars")
            .addPathSegment(jarId)
            .build();
        Request request = new Request.Builder().url(url).delete().build();
        executeRequest(request);
        LOGGER.info("Jar {} deleted.", jarId);
    }

    @Override
    public JobID run(JobParameters parameters) throws Exception {
        String programArgumentsString = parameters.getArguments().entrySet().stream()
            .map(e -> String.format("--%s %s", e.getKey(), e.getValue()))
            .collect(Collectors.joining(" "));

        HttpUrl url = urlBuilder()
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
            String jobId = runJarResponse.getJobid();
            LOGGER.info("Job submitted; jobId={}.", jobId);
            return JobID.fromHexString(jobId);
        } catch (Exception e) {
            LOGGER.error("Failed to submit job.", e);
            throw e;
        }
    }

    @Override
    public void cancel(JobID jobID) throws Exception {
        HttpUrl url = urlBuilder()
            .addPathSegment("jobs")
            .addPathSegment(jobID.toHexString())
            .build();
        Request request = new Request.Builder()
            .patch(RequestBody.create(new byte[0]))
            .url(url)
            .build();
        executeRequest(request);
    }

    @Override
    public JobStatus getStatus(JobID jobID) throws Exception {
        HttpUrl url = urlBuilder()
            .addPathSegment("jobs")
            .addPathSegment(jobID.toHexString())
            .build();
        Request request = new Request.Builder().get().url(url).build();
        JobStatusResponse statusResponse = executeRequest(request, JobStatusResponse.class);
        return JobStatus.valueOf(statusResponse.getState());
    }

    private HttpUrl.Builder urlBuilder() {
        return new HttpUrl.Builder()
            .scheme("http")
            .host(host)
            .port(port);
    }

    protected <T> T executeRequest(Request request, Class<T> responseClass) throws IOException {
        try (Response response = httpClient.newCall(request).execute()) {
            ResponseBody body = response.body();
            if (!response.isSuccessful()) {
                LOGGER.error("Request failed; code={}; body={}", response.code(),
                    body == null ? null : body.string());
                throw new RuntimeException("Request failed: " + response);
            }
            if (body == null) {
                throw new RuntimeException("Response body is empty.");
            }
            return objectMapper.readValue(body.string(), responseClass);
        }
    }

    protected void executeRequest(Request request) throws IOException {
        try (Response response = httpClient.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new RuntimeException("Request failed: " + response);
            }
        }
    }


    private static class JarsUploadResponse {
        private String filename;
        private String status;

        public String getFilename() {
            return filename;
        }

        public void setFilename(String filename) {
            this.filename = filename;
        }

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }
    }

    private static class RunJarResponse {
        private String jobid;

        public String getJobid() {
            return jobid;
        }

        public void setJobid(String jobid) {
            this.jobid = jobid;
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
    }
}
