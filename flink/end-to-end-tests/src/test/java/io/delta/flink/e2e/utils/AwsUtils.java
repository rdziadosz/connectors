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

package io.delta.flink.e2e.utils;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;

public class AwsUtils {

    public static void uploadDirectoryToS3(String targetBucketName,
                                           String s3Prefix,
                                           String localPath) throws InterruptedException {
        AmazonS3 s3Client = getClient();
        TransferManager manager = TransferManagerBuilder.standard().withS3Client(s3Client).build();
        MultipleFileUpload upload = manager
            .uploadDirectory(targetBucketName, s3Prefix, new File(localPath), true);
        upload.waitForCompletion();
    }

    public static void removeS3DirectoryRecursively(String bucketName, String prefix) {
        AmazonS3 s3Client = getClient();
        List<KeyVersion> toDelete = listRecursively(bucketName, prefix).stream()
            .map(o -> new KeyVersion(o.getKey()))
            .collect(Collectors.toList());
        DeleteObjectsRequest multiObjectDeleteRequest = new DeleteObjectsRequest(bucketName)
            .withKeys(toDelete)
            .withQuiet(false);
        s3Client.deleteObjects(multiObjectDeleteRequest);
    }

    public static List<S3ObjectSummary> listRecursively(String bucketName, String prefix) {
        AmazonS3 s3Client = getClient();
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
            .withBucketName(bucketName)
            .withPrefix(prefix);

        List<S3ObjectSummary> toDelete = new ArrayList<>();
        ObjectListing objectListing = s3Client.listObjects(listObjectsRequest);
        while (true) {
            toDelete.addAll(objectListing.getObjectSummaries());
            if (objectListing.isTruncated()) {
                objectListing = s3Client.listNextBatchOfObjects(objectListing);
            } else {
                break;
            }
        }
        return toDelete;
    }

    private static AmazonS3 getClient() {
        String awsRegion = System.getProperty("E2E_AWS_REGION");
        return awsRegion == null
            ? AmazonS3ClientBuilder.standard().build()
            : AmazonS3ClientBuilder.standard().withRegion(awsRegion).build();
    }

}
