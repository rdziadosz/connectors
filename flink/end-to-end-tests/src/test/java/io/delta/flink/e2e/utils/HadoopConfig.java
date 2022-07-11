package io.delta.flink.e2e.utils;

import org.apache.hadoop.conf.Configuration;

public class HadoopConfig {

    public static Configuration get() {
        Configuration conf = new Configuration();
        conf.set("parquet.compression", "SNAPPY");
        conf.set("io.delta.standalone.PARQUET_DATA_TIME_ZONE_ID", "UTC");
        return conf;
    }

}
