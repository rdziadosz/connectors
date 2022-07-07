package io.delta.flink.e2e;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class TestParameters {

    public static String getTestArtifactPath() {
        return getEnvProperty("E2E_JAR_PATH");
    }

    public static String getTestS3BucketName() {
        return getEnvProperty("E2E_S3_BUCKET_NAME");
    }

    public static String getTestDataLocalPath() {
        return getEnvProperty("E2E_TEST_DATA_LOCAL_PATH");
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
