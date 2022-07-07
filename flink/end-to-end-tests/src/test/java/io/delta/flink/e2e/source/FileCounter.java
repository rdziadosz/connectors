package io.delta.flink.e2e.source;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.delta.flink.e2e.utils.FileSystemUtils;
import org.apache.flink.shaded.guava18.com.google.common.util.concurrent.SimpleTimeLimiter;
import org.apache.flink.shaded.guava18.com.google.common.util.concurrent.TimeLimiter;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class FileCounter {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileCounter.class);

    private static final Duration CHECK_INTERVAL = Duration.ofSeconds(5);
    private static final Duration RECHECK_INTERVAL = Duration.ofSeconds(10);

    private final String path;
    private volatile int lastFileCount;

    FileCounter(String path) {
        this.path = path;
        this.lastFileCount = 0;
    }

    void waitForNewFiles(Duration timeout) throws Exception {
        TimeLimiter limiter = new SimpleTimeLimiter();
        int startingFileCount = lastFileCount;
        try {
            limiter.callWithTimeout((Callable<Void>) () -> {
                FileCounter.this.waitUntilTargetLocationIsCreated();
                FileCounter.this.waitForNewFiles();
                return null;
            }, timeout.toMillis(), TimeUnit.MILLISECONDS, true);
        } catch (TimeoutException e) {
            handleTimeout(startingFileCount);
        }
    }

    private void waitUntilTargetLocationIsCreated() throws Exception {
        while (!FileSystemUtils.locationExists(path)) {
            LOGGER.info("Target location does not exist yet. Waiting.");
            Thread.sleep(CHECK_INTERVAL.toMillis());
        }
    }

    private void waitForNewFiles() throws Exception {
        while (true) {
            int currentCount = FileSystemUtils.listFiles(path).size();
            if (lastFileCount > currentCount) {
                Assertions.fail("The number of output files has decreased.");
            } else if (lastFileCount == currentCount) {
                LOGGER.info("No new files have appeared. Waiting. [current={}]", lastFileCount);
                Thread.sleep(CHECK_INTERVAL.toMillis());
            } else { // lastFileCount < currentCount
                lastFileCount = currentCount;
                LOGGER.info("New files have appeared. Waiting a bit longer to ensure no new " +
                    "files will appear. [current={}]", currentCount);
                waitUntilNoNewFiles();
                return;
            }
        }
    }

    private void waitUntilNoNewFiles() throws Exception {
        while (true) {
            Thread.sleep(RECHECK_INTERVAL.toMillis());
            int currentCount = FileSystemUtils.listFiles(path).size();
            if (currentCount > lastFileCount) {
                LOGGER.info("New files have appeared again. Waiting a bit longer to ensure no " +
                    "new files will appear. [current={}]", currentCount);
                lastFileCount = currentCount;
            } else {
                LOGGER.info("No more new files have appeared. [current={}]", lastFileCount);
                return;
            }
        }
    }

    private void handleTimeout(int startingFileCount) {
        if (startingFileCount > lastFileCount) {
            Assertions.fail("The number of files have decreased.");
        } else if (startingFileCount == lastFileCount) {
            Assertions.fail("No new files have appeared within given timeout.");
        } else { // startingFileCount < lastFileCount
            Assertions.fail("New fails have been constantly appearing.");
        }
    }
}
