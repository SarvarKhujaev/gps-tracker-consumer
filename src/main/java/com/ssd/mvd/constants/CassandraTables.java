package com.ssd.mvd.constants;

import com.ssd.mvd.inspectors.StringOperations;

public enum CassandraTables {
    TABLETS,

    CARS,
    PATRULS,
    POLICE_TYPE,

    ESCORT {
        @Override
        @lombok.NonNull
        public String getCreateKeyspaceCommand() {
            return String.format(
                    """
                    %s %s %s
                    WITH REPLICATION = {
                        'class' : 'SimpleStrategy',
                        'replication_factor': 1
                    } AND DURABLE_WRITES = false;
                    """,

                    CassandraCommands.CREATE_KEYSPACE,
                    CassandraCommands.IF_NOT_EXISTS.replaceAll( ";", StringOperations.EMPTY ),

                    ESCORT
            );
        }
    },
    TUPLE_OF_CAR,
    ESCORT_LOCATION,

    TRACKERS,
    TRACKERSID,
    TRACKERS_LOCATION_TABLE,
    TRACKER_FUEL_CONSUMPTION;

    @lombok.NonNull
    public String getCreateKeyspaceCommand() {
        return String.format(
                """
                %s %s %s
                WITH REPLICATION = {
                    'class' : 'SimpleStrategy',
                    'replication_factor': 1
                } AND DURABLE_WRITES = false;
                """,

                CassandraCommands.CREATE_KEYSPACE,
                CassandraCommands.IF_NOT_EXISTS.replaceAll( ";", StringOperations.EMPTY ),

                TRACKERS
        );
    }
}
