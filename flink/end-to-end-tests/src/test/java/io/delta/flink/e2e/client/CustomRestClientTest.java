package io.delta.flink.e2e.client;

import com.github.tomakehurst.wiremock.WireMockServer;
import io.delta.flink.e2e.client.parameters.JobParameters;
import io.delta.flink.e2e.client.parameters.JobParametersBuilder;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static com.github.tomakehurst.wiremock.client.WireMock.aMultipart;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.absent;
import static com.github.tomakehurst.wiremock.client.WireMock.delete;
import static com.github.tomakehurst.wiremock.client.WireMock.deleteRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static com.github.tomakehurst.wiremock.client.WireMock.patch;
import static com.github.tomakehurst.wiremock.client.WireMock.patchRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;
import static io.delta.flink.e2e.utils.TestHelper.readTestFile;

/**
 * Tests for {@link CustomRestClient}.
 */
public class CustomRestClientTest {
    private static WireMockServer wireMockServer;
    private static final String SAMPLES_FOLDER = "/client/";
    private static final String host = "localhost";

    @BeforeAll
    public static void setUpAll() {
        wireMockServer = new WireMockServer();
        wireMockServer.start();
    }

    @AfterAll
    public static void cleanUpAll() {
        if (wireMockServer != null) {
            wireMockServer.stop();
        }
    }

    @Test
    void shouldUploadFile() throws Exception {
        // GIVEN
        wireMockServer.stubFor(
                post("/jars/upload")
                        .withMultipartRequestBody(
                                aMultipart()
                                        .withName("file")
                                        .withHeader("Content-Type", equalTo("application/x-java-archive"))
                        )
                        .willReturn(
                                aResponse()
                                        .withStatus(200)
                                        .withBody(readTestFile(SAMPLES_FOLDER + "Upload.json"))));

        FlinkClient flinkClient = FlinkClientFactory.getCustomRestClient(host, wireMockServer.port());

        // WHEN
        String jarId = flinkClient.uploadJar("src/test/resources/" + SAMPLES_FOLDER + "/test.jar");

        // THEN
        wireMockServer.verify(postRequestedFor(urlEqualTo("/jars/upload")));
        Assertions.assertEquals(jarId, "1dccf779-68c9-4909-8d99-ef181cdbfcd1_WordCount.jar");
    }

    @Test
    void shouldDeleteFile() throws Exception {
        // GIVEN
        wireMockServer.stubFor(delete(urlMatching("/jars/([\\w-.]*)")).willReturn(ok()));
        FlinkClient flinkClient = FlinkClientFactory.getCustomRestClient(host, wireMockServer.port());
        String mockJarId = "2d9d8032-a481-4d36-8077-c539e131bbae_WordCount.jar";

        // WHEN
        flinkClient.deleteJar(mockJarId);

        // THEN
        wireMockServer.verify(deleteRequestedFor(urlMatching("/jars/([\\w-.]*)")));
    }

    @Test
    void shouldCancelJob() throws Exception {
        // GIVEN
        wireMockServer.stubFor(patch(urlMatching("/jobs/([\\w-.]*)")).willReturn(ok()));
        FlinkClient flinkClient = FlinkClientFactory.getCustomRestClient(host, wireMockServer.port());
        JobID mockJobId = JobID.fromHexString("a52ff9be21027aeab2008fa300f13359");

        // WHEN
        flinkClient.cancel(mockJobId);

        // THEN
        wireMockServer.verify(patchRequestedFor(urlMatching("/jobs/([\\w-.]*)")));
    }

    @Test
    void shouldGetJobStatus() throws Exception {
        // GIVEN
        wireMockServer.stubFor(
                get(urlMatching("/jobs/([\\w-.]*)"))
                        .willReturn(
                                aResponse()
                                        .withStatus(200)
                                        .withBody(readTestFile(SAMPLES_FOLDER + "JobStatus.json"))));
        FlinkClient flinkClient = FlinkClientFactory.getCustomRestClient(host, wireMockServer.port());
        JobID mockJobId = JobID.fromHexString("a52ff9be21027aeab2008fa300f13359");

        // WHEN
        JobStatus status = flinkClient.getStatus(mockJobId);

        // THEN
        wireMockServer.verify(getRequestedFor(urlMatching("/jobs/([\\w-.]*)")));
    }

    @Test
    void shouldRunJob() throws Exception {
        // GIVEN
        wireMockServer.stubFor(post(urlMatching("/jars/([\\w-.]*)/run.*"))
                .withQueryParam("allowNonRestoredState", matching(".*").or(absent()))
                .withQueryParam("savepointPath", matching(".*").or(absent()))
                .withQueryParam("program-args", matching(".*").or(absent()))
                .withQueryParam("programArg", matching(".*").or(absent()))
                .withQueryParam("entry-class", matching(".*").or(absent()))
                .withQueryParam("parallelism", matching("\\d*").or(absent()))
                .willReturn(
                        aResponse()
                                .withStatus(200)
                                .withBody(readTestFile(SAMPLES_FOLDER + "Run.json"))));

        FlinkClient flinkClient = FlinkClientFactory.getCustomRestClient(host, wireMockServer.port());
        String mockJarId = "2d9d8032-a481-4d36-8077-c539e131bbae_WordCount.jar";

        JobParameters jobParameters = JobParametersBuilder
                .builder()
                .withParallelism(1)
                .withEntryPointClassName("org.apache.flink.examples.java.wordcount.WordCount")
                .withJarId(mockJarId)
                .build();

        // WHEN
        JobID jobID = flinkClient.run(jobParameters);

        // THEN
        wireMockServer.verify(postRequestedFor(urlMatching("/jars/([\\w-.]*)/run.*")));
        Assertions.assertEquals(jobID, JobID.fromHexString("a52ff9be21027aeab2008fa300f13359"));
    }
}