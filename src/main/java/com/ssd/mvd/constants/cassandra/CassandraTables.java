package com.ssd.mvd.constants.cassandra;

import com.ssd.mvd.inspectors.dataTypesInpectors.StringOperations;

public enum CassandraTables {
    TABLETS,

    CARS,
    PATRULS,
    PATRUL_CAR_DATA,
    PATRUL_FIO_DATA,
    PATRUL_TASK_DATA,
    PATRUL_REGION_DATA,
    PATRUL_UNIQUE_DATA,
    PATRUL_LOCATION_DATA,

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
