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

import io.delta.flink.e2e.client.parameters.JobParameters;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;

/**
 * {@link FlinkClient} provides an API to manage jobs on Flink cluster.
 *
 * Implementations of this interface should encapsulate the logic for communicating with the
 * Flink API.
 */
public interface FlinkClient {

    /**
     * Uploads a jar to the Flink cluster.
     *
     * @param jarPath path to jar file to be uploaded
     * @return string value that identifies uploaded jar file
     */
    String uploadJar(String jarPath) throws Exception;

    /**
     * Deletes a jar file from Flink cluster.
     *
     * @param jarId string value that identifies jar file
     */
    void deleteJar(String jarId) throws Exception;

    /**
     * Submits a Flink job.
     *
     * @param parameters parameters necessary to run the Flink job
     * @return value that identifies a job
     */
    JobID run(JobParameters parameters) throws Exception;

    /**
     * Terminates a Flink job.
     *
     * @param jobID value that identifies a job
     */
    void cancel(JobID jobID) throws Exception;

    /**
     * Returns the current status of a job execution.
     *
     * @param jobID value that identifies a job
     * @return current status of execution
     */
    JobStatus getStatus(JobID jobID) throws Exception;

    /**
     * Checks information about the job status and returns information whether the job has
     * finished.
     *
     * @param jobID value that identifies a job
     * @return whether the job has been finished
     */
    default boolean isFinished(JobID jobID) throws Exception {
        return getStatus(jobID).equals(JobStatus.FINISHED);
    }

    /**
     * Checks information about the job status and returns information whether the job has
     * failed.
     *
     * @param jobID value that identifies a job
     * @return whether the job has been failed
     */
    default boolean isFailed(JobID jobID) throws Exception {
        return getStatus(jobID).equals(JobStatus.FAILED);
    }

    /**
     * Checks information about the job status and returns information whether the job has
     * been canceled.
     *
     * @param jobID value that identifies a job
     * @return whether the job has been cancelled
     */
    default boolean isCanceled(JobID jobID) throws Exception {
        return getStatus(jobID).equals(JobStatus.CANCELED);
    }

}
