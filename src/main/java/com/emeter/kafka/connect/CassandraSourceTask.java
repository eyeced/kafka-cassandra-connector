package com.emeter.kafka.connect;

import com.emeter.kafka.data.Command;
import com.emeter.kafka.data.Read;
import com.emeter.kafka.serialize.JsonSerializer;
import com.emeter.kafka.store.CassandraStore;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.Selector;
import java.sql.Date;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Created by abhiso on 4/25/16.
 */
public class CassandraSourceTask extends SourceTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraSourceTask.class);

    CassandraStore cassandraStore;

    JsonSerializer serializer = new JsonSerializer();
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
    }

    /**
     * Poll this SourceTask for new records. This method should block if no data is currently
     * available.
     *
     * @return a list of source records
     */
    @Override
    public List<SourceRecord> poll() throws InterruptedException {

        return cassandraStore.get("select distinct meter_id from reads", new Object[]{}, row -> row.getLong("meter_id"))
                .flatMap(meterId -> cassandraStore.get(
                        "select * from reads where meter_id = ? and meas_type_id = ? and read_time < ? LIMIT 1",
                        new Object[] {meterId, 1l, Date.from(Instant.now())},
                        row -> new Read(row.getLong("meter_id"), row.getLong("meas_type_id"), row.getDouble("value"), row.getTimestamp("read_time"), row.getLong("value"))))
                .filter(read -> read.getValue() < 0.10)
                .map(read -> new Command("main", read.getMeterId()))
                .map(command -> new SourceRecord(null, null, "commands", Schema.STRING_SCHEMA, serializer.toJson(command)))
                .toList()
                .toBlocking()
                .first();
    }

    /**
     * Signal this SourceTask to stop. In SourceTasks, this method only needs to signal to the task that it should stop
     * trying to poll for new data and interrupt any outstanding poll() requests. It is not required that the task has
     * fully stopped. Note that this method necessarily may be invoked from a different thread than {@link #poll()} and
     * {@link #commit()}.
     * <p>
     * For example, if a task uses a {@link Selector} to receive data over the network, this method
     * could set a flag that will force {@link #poll()} to exit immediately and invoke
     * {@link Selector#wakeup() wakeup()} to interrupt any ongoing requests.
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
