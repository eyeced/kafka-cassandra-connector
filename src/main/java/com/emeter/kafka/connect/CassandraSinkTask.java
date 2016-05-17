package com.emeter.kafka.connect;

import com.emeter.kafka.data.Read;
import com.emeter.kafka.serialize.JsonSerializer;
import com.emeter.kafka.store.CassandraStore;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.util.Collection;
import java.util.Map;

/**
 * Created by abhiso on 4/25/16.
 */
public class CassandraSinkTask extends SinkTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraSinkTask.class);

    private JsonSerializer<Read> serializer;

    private CassandraStore cassandraStore;
    /**
     * Start the Task. This should handle any configuration parsing and one-time setup of the task.
     *
     * @param props initial configuration
     */
    @Override
    public void start(Map<String, String> props) {

        String keyspace = props.get(CassandraSinkConnector.KEYSPACE);
        cassandraStore = new CassandraStore();
        cassandraStore.init("localhost", keyspace);
        serializer = new JsonSerializer<>();
    }

    /**
     * Put the records in the sink. Usually this should send the records to the sink asynchronously
     * and immediately return.
     * <p>
     * If this operation fails, the SinkTask may throw a {@link RetriableException} to
     * indicate that the framework should attempt to retry the same call again. Other exceptions will cause the task to
     * be stopped immediately. {@link org.apache.kafka.connect.sink.SinkTaskContext#timeout(long)} can be used to set the maximum time before the
     * batch will be retried.
     *
     * @param records the set of records to send
     */
    @Override
    public void put(Collection<SinkRecord> records) {
        Observable.from(records)
                .map(sinkRecord -> {
                    LOGGER.info(String.valueOf(sinkRecord.value()));
                    return serializer.fromJson((String) sinkRecord.value(), Read.class);
                })
                .flatMap(read -> cassandraStore.insert(
                        Observable.just(read),
                        "insert into reads(meter_id, meas_type_id, value, read_time, flag) values(?, ?, ?, ?, ?)",
                        r -> new Object[] {r.getMeterId(), r.getMeasTypeId(), r.getValue(), r.getReadTime(), r.getFlag() }))
                .subscribe(read -> {}, err -> {
                    LOGGER.error(err.toString(), err);
                    throw new RetriableException(err);
                }, () -> {});
    }

    /**
     * Flush all records that have been {@link #put} for the specified topic-partitions. The
     * offsets are provided for convenience, but could also be determined by tracking all offsets
     * included in the SinkRecords passed to {@link #put}.
     *
     * @param offsets mapping of TopicPartition to committed offset
     */
    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {}

    /**
     * Perform any cleanup to stop this task. In SinkTasks, this method is invoked only once outstanding calls to other
     * methods have completed (e.g., {@link #put(Collection)} has returned) and a final {@link #flush(Map)} and offset
     * commit has completed. Implementations of this method should only need to perform final cleanup operations, such
     * as closing network connections to the sink system.
     */
    @Override
    public void stop() {
        cassandraStore.close();
    }

    /**
     * Get the version of this task. Usually this should be the same as the corresponding {@link org.apache.kafka.connect.connector.Connector} class's version.
     *
     * @return the version, formatted as a String
     */
    @Override
    public String version() {
        return null;
    }
}
