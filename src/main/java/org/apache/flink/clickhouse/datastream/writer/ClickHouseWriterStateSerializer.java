package org.apache.flink.clickhouse.datastream.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;

/** {@link ClickHouseWriterState} serializer. */
@Internal
public class ClickHouseWriterStateSerializer
        implements SimpleVersionedSerializer<ClickHouseWriterState> {

    @Override
    public int getVersion() {
        return 0;
    }

    @Override
    public byte[] serialize(ClickHouseWriterState obj) throws IOException {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public ClickHouseWriterState deserialize(int version, byte[] serialized) throws IOException {
        throw new UnsupportedOperationException("Not implemented yet.");
    }
}
