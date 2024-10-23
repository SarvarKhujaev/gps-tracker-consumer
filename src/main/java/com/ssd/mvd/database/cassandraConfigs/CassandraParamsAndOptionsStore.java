package com.ssd.mvd.database.cassandraConfigs;

import com.datastax.driver.core.*;
import com.ssd.mvd.database.cassandraRegistry.CassandraConverter;

public class CassandraParamsAndOptionsStore extends CassandraConverter {
    protected CassandraParamsAndOptionsStore () {}

    protected final String CLUSTER_NAME = checkContextOrReturnDefaultValue(
            "variables.CASSANDRA_VARIABLES.CASSANDRA_CLUSTER_NAME",
            "TEST_CLUSTER"
    );

    protected final String HOST = checkContextOrReturnDefaultValue(
            "variables.CASSANDRA_VARIABLES.CASSANDRA_HOST",
            "localhost"
    );

    protected final synchronized PoolingOptions getPoolingOptions () {
        return new PoolingOptions()
                .setConnectionsPerHost(
                        HostDistance.LOCAL,
                        checkContextOrReturnDefaultValue(
                                "variables.CASSANDRA_VARIABLES.CASSANDRA_CORE_CONN_LOCAL",
                                ProtocolOptions.DEFAULT_MAX_SCHEMA_AGREEMENT_WAIT_SECONDS
                        ),
                        checkContextOrReturnDefaultValue(
                                "variables.CASSANDRA_VARIABLES.CASSANDRA_MAX_CONN_LOCAL",
                                ProtocolOptions.DEFAULT_MAX_SCHEMA_AGREEMENT_WAIT_SECONDS
                        )
                ).setMaxConnectionsPerHost(
                        HostDistance.LOCAL,
                        checkContextOrReturnDefaultValue(
                                "variables.CASSANDRA_VARIABLES.CASSANDRA_MAX_CONN_LOCAL",
                                ProtocolOptions.DEFAULT_MAX_SCHEMA_AGREEMENT_WAIT_SECONDS
                        )
                ).setConnectionsPerHost(
                        HostDistance.REMOTE,
                        checkContextOrReturnDefaultValue(
                                "variables.CASSANDRA_VARIABLES.CASSANDRA_CORE_CONN_REMOTE",
                                ProtocolOptions.DEFAULT_MAX_SCHEMA_AGREEMENT_WAIT_SECONDS
                        ),
                        checkContextOrReturnDefaultValue(
                                "variables.CASSANDRA_VARIABLES.CASSANDRA_MAX_CONN_REMOTE",
                                ProtocolOptions.DEFAULT_MAX_SCHEMA_AGREEMENT_WAIT_SECONDS
                        )
                ).setMaxConnectionsPerHost(
                        HostDistance.REMOTE,
                        checkContextOrReturnDefaultValue(
                                "variables.CASSANDRA_VARIABLES.CASSANDRA_MAX_CONN_REMOTE",
                                ProtocolOptions.DEFAULT_MAX_SCHEMA_AGREEMENT_WAIT_SECONDS
                        )
                ).setMaxRequestsPerConnection(
                        HostDistance.LOCAL,
                        checkContextOrReturnDefaultValue(
                                "variables.CASSANDRA_VARIABLES.CASSANDRA_MAX_REQUESTS_PER_CONNECTION_LOCAL",
                                ProtocolOptions.DEFAULT_MAX_SCHEMA_AGREEMENT_WAIT_SECONDS
                        )
                ).setMaxRequestsPerConnection(
                        HostDistance.REMOTE,
                        checkContextOrReturnDefaultValue(
                                "variables.CASSANDRA_VARIABLES.CASSANDRA_MAX_REQUESTS_PER_CONNECTION_REMOTE",
                                ProtocolOptions.DEFAULT_MAX_SCHEMA_AGREEMENT_WAIT_SECONDS
                        )
                ).setPoolTimeoutMillis(
                        checkContextOrReturnDefaultValue(
                                "variables.CASSANDRA_VARIABLES.CASSANDRA_MAX_CONN_REMOTE",
                                ProtocolOptions.DEFAULT_MAX_SCHEMA_AGREEMENT_WAIT_SECONDS
                        )
                ).setMaxQueueSize(
                        checkContextOrReturnDefaultValue(
                                "variables.CASSANDRA_VARIABLES.CASSANDRA_MAX_REQ",
                                ProtocolOptions.DEFAULT_MAX_SCHEMA_AGREEMENT_WAIT_SECONDS
                        )
                ).setIdleTimeoutSeconds(
                        checkContextOrReturnDefaultValue(
                                "variables.CASSANDRA_VARIABLES.CASSANDRA_IDLE_CONN_TIME_IN_SECONDS",
                                ProtocolOptions.DEFAULT_MAX_SCHEMA_AGREEMENT_WAIT_SECONDS
                        )
                ).setHeartbeatIntervalSeconds(
                        checkContextOrReturnDefaultValue(
                                "variables.CASSANDRA_VARIABLES.CASSANDRA_HEARTBEAT_INTERVAL_IN_SECONDS",
                                ProtocolOptions.DEFAULT_MAX_SCHEMA_AGREEMENT_WAIT_SECONDS
                        )
                );
    }

    protected final synchronized SocketOptions getSocketOptions () {
        return new SocketOptions()
                .setConnectTimeoutMillis(
                        checkContextOrReturnDefaultValue(
                                "variables.CASSANDRA_VARIABLES.CASSANDRA_CONNECTION_TIMEOUT_IN_MILLIS",
                                ProtocolOptions.DEFAULT_MAX_SCHEMA_AGREEMENT_WAIT_SECONDS
                        )
                ).setReadTimeoutMillis(
                        checkContextOrReturnDefaultValue(
                                "variables.CASSANDRA_VARIABLES.CASSANDRA_READ_TIMEOUT_IN_MILLIS",
                                ProtocolOptions.DEFAULT_MAX_SCHEMA_AGREEMENT_WAIT_SECONDS
                        )
                ).setTcpNoDelay( true )
                .setKeepAlive( true );
    }

    protected final synchronized int getPort () {
        return checkContextOrReturnDefaultValue(
                "variables.CASSANDRA_VARIABLES.CASSANDRA_PORT",
                ProtocolOptions.DEFAULT_PORT
        );
    }

    protected final synchronized CustomRetryPolicy getCustomRetryPolicy () {
        return CustomRetryPolicy.generate(
                checkContextOrReturnDefaultValue(
                        "variables.CASSANDRA_VARIABLES.CASSANDRA_RETRY_UNAVAILABLE_ATTEMPTS",
                        ProtocolOptions.DEFAULT_MAX_SCHEMA_AGREEMENT_WAIT_SECONDS
                ),
                checkContextOrReturnDefaultValue(
                        "variables.CASSANDRA_VARIABLES.CASSANDRA_RETRY_WRITE_ATTEMPTS",
                        ProtocolOptions.DEFAULT_MAX_SCHEMA_AGREEMENT_WAIT_SECONDS
                ),
                checkContextOrReturnDefaultValue(
                        "variables.CASSANDRA_VARIABLES.CASSANDRA_RETRY_READ_ATTEMPTS",
                        ProtocolOptions.DEFAULT_MAX_SCHEMA_AGREEMENT_WAIT_SECONDS
                )
        );
    }

    protected final synchronized QueryOptions getQueryOptions () {
        return new QueryOptions()
                .setDefaultIdempotence( true )
                .setConsistencyLevel( ConsistencyLevel.ONE );
    }
}
