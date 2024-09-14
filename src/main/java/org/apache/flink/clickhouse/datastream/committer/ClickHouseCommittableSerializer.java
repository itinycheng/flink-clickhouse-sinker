package org.apache.flink.clickhouse.datastream.committer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;

/** {@link ClickHouseCommittable} serializer. */
@Internal
public class ClickHouseCommittableSerializer
        implements SimpleVersionedSerializer<ClickHouseCommittable> {

    @Override
    public int getVersion() {
        return 0;
    }

    @Override
    public byte[] serialize(ClickHouseCommittable obj) throws IOException {
        throw new UnsupportedOperationException("Not supported yet");
    }

    @Override
    public ClickHouseCommittable deserialize(int version, byte[] serialized) throws IOException {
        throw new UnsupportedOperationException("Not supported yet");
    }
}
