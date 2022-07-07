package io.delta.flink.e2e.source;

class HadoopConfig {

    static org.apache.hadoop.conf.Configuration get() {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.ContainerCredentialsProvider");
        return conf;
    }

}
