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

package io.delta.flink.e2e;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class TestParameters {

    public static String getTestArtifactPath() {
        return getEnvProperty("E2E_JAR_PATH");
    }

    public static String getTestS3BucketName() {
        return getEnvProperty("E2E_S3_BUCKET_NAME");
    }

    public static String getJobManagerHost() {
        return getEnvProperty("E2E_JOBMANAGER_HOSTNAME");
    }

    public static int getJobManagerPort() {
        String jobmanagerPortString = getEnvProperty("E2E_JOBMANAGER_PORT");
        return Integer.parseInt(jobmanagerPortString);
    }

    public static boolean preserveS3Data() {
        String preserveS3Data = System.getProperty("E2E_PRESERVE_S3_DATA");
        return "yes".equalsIgnoreCase(preserveS3Data);
    }

    private static String getEnvProperty(String name) {
        String property = System.getProperty(name);
        assertNotNull(property, name + " environment property has not been specified.");
        return property;
    }

}
