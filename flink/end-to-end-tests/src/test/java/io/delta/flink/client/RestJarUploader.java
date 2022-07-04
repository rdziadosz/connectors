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

import java.io.File;
import java.io.IOException;

import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.Request;
import okhttp3.RequestBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RestJarUploader extends RestClientBase implements JarUploader {

    private static final Logger LOGGER = LoggerFactory.getLogger(CustomRestClient.class);

    RestJarUploader(String host, int port) {
        super(host, port);
    }

    @Override
    public String uploadJar(String jarPath) throws IOException {
        LOGGER.info("Uploading jar: {}", jarPath);

        HttpUrl url = new HttpUrl.Builder()
            .scheme("http")
            .host(host)
            .port(port)
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
        HttpUrl url = new HttpUrl.Builder()
            .scheme("http")
            .host(host)
            .port(port)
            .addPathSegment("jars")
            .addPathSegment(jarId)
            .build();
        Request request = new Request.Builder().url(url).delete().build();
        executeRequest(request);
        LOGGER.info("Jar {} deleted.", jarId);
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

        @Override
        public String toString() {
            return "JarsUploadResponse{filename='" + filename + "', status='" + status + "'}";
        }
    }

}
