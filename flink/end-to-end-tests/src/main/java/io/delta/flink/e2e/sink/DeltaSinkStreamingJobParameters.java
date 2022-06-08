package io.delta.flink.e2e.sink;

import org.apache.flink.api.common.restartstrategy.RestartStrategies.RestartStrategyConfiguration;

public class DeltaSinkStreamingJobParameters {

    private final String deltaTablePath;
    private final boolean isPartitioned;
    private final RestartStrategyConfiguration restartStrategy;

    private final int sourceParallelism;
    private final int sinkParallelism;
    private final int inputRecordsCount;

    public DeltaSinkStreamingJobParameters(String deltaTablePath,
                                           boolean isPartitioned,
                                           RestartStrategyConfiguration restartStrategy,
                                           int sourceParallelism,
                                           int sinkParallelism,
                                           int inputRecordsCount) {
        this.deltaTablePath = deltaTablePath;
        this.isPartitioned = isPartitioned;
        this.restartStrategy = restartStrategy;
        this.sourceParallelism = sourceParallelism;
        this.sinkParallelism = sinkParallelism;
        this.inputRecordsCount = inputRecordsCount;
    }

    public String getDeltaTablePath() {
        return deltaTablePath;
    }

    public boolean isPartitioned() {
        return isPartitioned;
    }

    public RestartStrategyConfiguration getRestartStrategy() {
        return restartStrategy;
    }

    public int getSourceParallelism() {
        return sourceParallelism;
    }

    public int getSinkParallelism() {
        return sinkParallelism;
    }

    public int getInputRecordsCount() {
        return inputRecordsCount;
    }


    public static final class DeltaSinkStreamingJobParametersBuilder {
        private String deltaTablePath;
        private boolean isPartitioned;
        private RestartStrategyConfiguration restartStrategy;
        private int sourceParallelism;
        private int sinkParallelism;
        private int inputRecordsCount;

        private DeltaSinkStreamingJobParametersBuilder() {
        }

        public static DeltaSinkStreamingJobParametersBuilder deltaSinkStreamingJobParameters() {
            return new DeltaSinkStreamingJobParametersBuilder();
        }

        public DeltaSinkStreamingJobParametersBuilder withDeltaTablePath(String deltaTablePath) {
            this.deltaTablePath = deltaTablePath;
            return this;
        }

        public DeltaSinkStreamingJobParametersBuilder withIsPartitioned(boolean isPartitioned) {
            this.isPartitioned = isPartitioned;
            return this;
        }

        public DeltaSinkStreamingJobParametersBuilder withRestartStrategy(
            RestartStrategyConfiguration restartStrategy) {
            this.restartStrategy = restartStrategy;
            return this;
        }

        public DeltaSinkStreamingJobParametersBuilder withSourceParallelism(int sourceParallelism) {
            this.sourceParallelism = sourceParallelism;
            return this;
        }

        public DeltaSinkStreamingJobParametersBuilder withSinkParallelism(int sinkParallelism) {
            this.sinkParallelism = sinkParallelism;
            return this;
        }

        public DeltaSinkStreamingJobParametersBuilder withInputRecordsCount(int inputRecordsCount) {
            this.inputRecordsCount = inputRecordsCount;
            return this;
        }

        public DeltaSinkStreamingJobParameters build() {
            return new DeltaSinkStreamingJobParameters(deltaTablePath, isPartitioned,
                restartStrategy, sourceParallelism, sinkParallelism, inputRecordsCount);
        }
    }
}
