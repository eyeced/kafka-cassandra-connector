package com.emeter.kafka.connect;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by abhiso on 4/25/16.
 */
public class CassandraSinkConnector extends SinkConnector {

    public static final String TOPIC_CONFIG = "topic";
    public static final String KEYSPACE = "myconnect";

    /**
     * Get the version of this connector.
     *
     * @return the version, formatted as a String
     */
    @Override
    public String version() {
        return null;
    }

    /**
     * Start this Connector. This method will only be called on a clean Connector, i.e. it has
     * either just been instantiated and initialized or {@link #stop()} has been invoked.
     *
     * @param props configuration settings
     */
    @Override
    public void start(Map<String, String> props) {

    }

    /**
     * Returns the Task implementation for this Connector.
     */
    @Override
    public Class<? extends Task> taskClass() {
        return CassandraSinkTask.class;
    }

    /**
     * Returns a set of configurations for Tasks based on the current configuration,
     * producing at most count configurations.
     *
     * @param maxTasks maximum number of configurations to generate
     * @return configurations for Tasks
     */
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        Map<String, String> map = new HashMap<>();
        map.put(KEYSPACE, "myconnect");
        return Arrays.asList(map);
    }

    /**
     * Stop this connector.
     */
    @Override
    public void stop() {

    }
}
