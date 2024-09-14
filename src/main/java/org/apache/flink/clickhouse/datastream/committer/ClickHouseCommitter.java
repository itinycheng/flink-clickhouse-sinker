package org.apache.flink.clickhouse.datastream.committer;

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.clickhouse.internal.connection.ClickHouseConnectionProvider;

import java.io.IOException;
import java.util.Collection;

/** clickhouse committer. */
public class ClickHouseCommitter implements Committer<ClickHouseCommittable> {

    private final DeliveryGuarantee deliveryGuarantee;

    private final ClickHouseConnectionProvider connectionProvider;

    public ClickHouseCommitter(
            DeliveryGuarantee deliveryGuarantee, ClickHouseConnectionProvider connectionProvider) {
        this.deliveryGuarantee = deliveryGuarantee;
        this.connectionProvider = connectionProvider;
    }

    @Override
    public void commit(Collection<CommitRequest<ClickHouseCommittable>> commitRequests)
            throws IOException, InterruptedException {
        throw new UnsupportedOperationException("Not supported yet");
    }

    @Override
    public void close() throws Exception {
        if (this.connectionProvider != null) {
            this.connectionProvider.closeConnections();
        }
    }
}
