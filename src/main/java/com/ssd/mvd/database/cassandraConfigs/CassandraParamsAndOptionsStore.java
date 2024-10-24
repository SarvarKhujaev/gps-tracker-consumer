package com.ssd.mvd.database.cassandraConfigs;

import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.*;

import com.ssd.mvd.inspectors.CassandraConverter;
import com.ssd.mvd.database.CassandraDataControl;

import com.ssd.mvd.annotations.EntityConstructorAnnotation;
import com.ssd.mvd.interfaces.ServiceCommonMethods;

import com.ssd.mvd.inspectors.AnnotationInspector;
import com.ssd.mvd.inspectors.EntitiesInstances;
import com.ssd.mvd.inspectors.UuidInspector;

import java.lang.ref.WeakReference;

@com.ssd.mvd.annotations.ImmutableEntityAnnotation
@com.ssd.mvd.annotations.ServiceParametrAnnotation( propertyGroupName = "CASSANDRA_VARIABLES" )
public class CassandraParamsAndOptionsStore extends CassandraConverter implements ServiceCommonMethods {
    @EntityConstructorAnnotation( permission = CassandraDataControl.class )
    protected <T extends UuidInspector> CassandraParamsAndOptionsStore (@lombok.NonNull final Class<T> instance ) {
        super( CassandraParamsAndOptionsStore.class );

        AnnotationInspector.checkCallerPermission( instance, CassandraParamsAndOptionsStore.class );
        AnnotationInspector.checkAnnotationIsImmutable( CassandraParamsAndOptionsStore.class );
    }

    protected final static String CASSANDRA_CLUSTER_NAME = getVariable(
            CassandraParamsAndOptionsStore.class,
            CassandraParamsAndOptionsStore.class.getDeclaredFields()[1].getName()
    );

    protected final static String CASSANDRA_HOST = checkContextOrReturnDefaultValue(
            getVariable(
                    CassandraParamsAndOptionsStore.class,
                    CassandraParamsAndOptionsStore.class.getDeclaredFields()[2].getName()
            ),
            "localhost"
    );

    protected final static WeakReference< QueryOptions > QUERY_OPTIONS = EntitiesInstances.generateWeakEntity(
            new QueryOptions()
                    .setDefaultIdempotence( true )
                    .setConsistencyLevel( ConsistencyLevel.ONE )
    );

    protected final static int CASSANDRA_PORT = checkContextOrReturnDefaultValue(
            "variables.CASSANDRA_VARIABLES.CASSANDRA_PORT",
            ProtocolOptions.DEFAULT_PORT
    );

    protected final static WeakReference< SocketOptions > SOCKET_OPTIONS = EntitiesInstances.generateWeakEntity(
            new SocketOptions()
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
                    .setKeepAlive( true )
    );

    protected final static CustomRetryPolicy CUSTOM_RETRY_POLICY =
            CustomRetryPolicy.generate(
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

    protected final static LoadBalancingPolicy CUSTOM_LOAD_BALANCING = new DCAwareRoundRobinPolicy
            .Builder()
            .withLocalDc( CASSANDRA_HOST )
            .build();

    protected final static WeakReference< PlainTextAuthProvider > AUTH_PROVIDER = EntitiesInstances.generateWeakEntity(
            new PlainTextAuthProvider( EMPTY, CASSANDRA_HOST )
    );

    @lombok.NonNull
    @lombok.Synchronized
    protected final synchronized WeakReference< PoolingOptions > getPoolingOptions () {
        return EntitiesInstances.generateWeakEntity(
                new PoolingOptions()
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
                                        PoolingOptions.DEFAULT_IDLE_TIMEOUT_SECONDS
                                )
                        ).setHeartbeatIntervalSeconds(
                                checkContextOrReturnDefaultValue(
                                        "variables.CASSANDRA_VARIABLES.CASSANDRA_HEARTBEAT_INTERVAL_IN_SECONDS",
                                        PoolingOptions.DEFAULT_HEARTBEAT_INTERVAL_SECONDS
                                )
                        )
        );
    }

    @Override
    public void close() {
        CUSTOM_RETRY_POLICY.close();
        CUSTOM_LOAD_BALANCING.close();

        clearReference( AUTH_PROVIDER );

        super.logging( this );

        super.clearAllEntities();
    }
}
