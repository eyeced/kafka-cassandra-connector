package com.emeter.kafka.store;


import com.datastax.driver.core.*;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.functions.Func1;
import rx.schedulers.Schedulers;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Cassandra Store class
 * for access
 * Created by abhinav on 4/11/15.
 */
public class CassandraStore {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(CassandraStore.class);

    private Cluster cluster;

    private Session session;

    private Integer port = 9042;

    // keep an map of sql and bound statement, performance optimization
    private Map<String, PreparedStatement> sqlStatementMap = new ConcurrentHashMap<>();

    public CassandraStore() {}

    public void init(String contactPoints, String keyspace) {
        Cluster cluster = Cluster.builder().addContactPoints(contactPoints.split(",")).withPort(port).build();
        session = cluster.connect(keyspace);
    }

    /**
     * executing an async parallel write to cassandra
     *
     * @param data stream of data coming
     * @param sql insert sql statement
     * @param mapperFn function which creates object to the the object array that would bind to the statement values
     * @param <T> BaseReading type
     * @return ResultSet of the inserted data.
     */
    public <T> Observable<T> insert(Observable<T> data, String sql,
                                    Func1<T, Object[]> mapperFn) {
        return data.flatMap(t -> executeSql(getBoundStatement(sql), mapperFn.call(t))
                        .flatMap(rs -> Observable.just(t)));
    }

    /**
     * delete data from cassandra
     * @param sql delete sql
     * @return void observable
     */
    public Observable<Void> delete(String sql, Object[] args) {
        return Observable.create(subscriber ->
            executeSql(getBoundStatement(sql), args).subscribe(rs -> {
                if (rs.isFullyFetched()) {
                    subscriber.onNext(null);
                }
                subscriber.onCompleted();
            }, throwable -> subscriber.onError(throwable))
        );
    }

    /**
     * execute the fetch call for the given query
     *
     * @param sql sql statement to be run
     * @param args args to be set in the statement
     * @param mapperFn function which converts the row into object T
     * @param <T> type T which extends BaseReading
     * @return observable of T
     */
    public <T> Observable<T> get(String sql, Object[] args, Func1<Row, T> mapperFn) {
//        LOGGER.info("SQL [" + sql + "]");
        return executeSql(getBoundStatement(sql), args)
                .flatMapIterable(rows -> rows)
                .map(mapperFn);
    }

    /**
     * execute a query on the cassandra db in async manner
     *
     * @param statement bound statement to run
     * @param args args that are to be set on the query
     * @return Observable of the result set
     */
    public Observable<ResultSet> executeSql(BoundStatement statement, Object[] args) {
        // start a scheduler.io which would start these insert statements in parallel
        ResultSet resultSetFuture = session.execute(statement.bind(args));
//        LOGGER.info("args - " + Arrays.asList(args).toString() + " - " + Thread.currentThread().getName());
//        LOGGER.info(statement.preparedStatement().getQueryString());
        return Observable.just(resultSetFuture);
    }

    public ResultSet executeCql(BoundStatement statement, Object[] args) {
        return session.execute(statement.bind(args));
    }

    /**
     * execute sql asynchronously
     * @param sql sql to execute
     * @return resultSet stream
     */
    public Observable<ResultSet> executeSql(String sql) {
        Scheduler scheduler = Schedulers.io();
        ListenableFuture<ResultSet> future = session.executeAsync(sql);
        return Observable.from(future, scheduler);
    }

    /**
     * clean table
     * @param tableName table ro be cleaned
     * @return observable void
     */
    public Observable<Boolean> clearTable(String... tableName) {
        return Observable.from(tableName)
                .flatMap(s -> executeSql("TRUNCATE " + s)).map(rs -> rs.isFullyFetched());
    }

    /**
     * helper function to get the bound statement for the sql.
     * @param sql sql added
     * @return the bound statement
     */
    private BoundStatement getBoundStatement(String sql) {
        if (!sqlStatementMap.containsKey(sql)) {
            PreparedStatement statement = session.prepare(sql);
            statement.setConsistencyLevel(ConsistencyLevel.QUORUM);
            sqlStatementMap.put(sql, statement);
        }
        return new BoundStatement(sqlStatementMap.get(sql));
    }

    public void close() {
        session.close();
    }
}
