package io.delta.flink.e2e.source;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.table.data.RowData;

class SinglePartitionBucketAssigner implements BucketAssigner<RowData, String> {
    @Override
    public String getBucketId(RowData element, Context context) {
        return "all";
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
        return SimpleVersionedStringSerializer.INSTANCE;
    }
}
