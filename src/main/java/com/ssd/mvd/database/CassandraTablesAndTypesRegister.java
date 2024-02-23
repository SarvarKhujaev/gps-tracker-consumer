package com.ssd.mvd.database;

import com.ssd.mvd.constants.CassandraDataTypes;
import com.ssd.mvd.constants.CassandraCommands;
import com.ssd.mvd.constants.CassandraFunctions;
import com.ssd.mvd.constants.CassandraTables;

import com.datastax.driver.core.Session;
import com.ssd.mvd.entity.TupleOfCar;

import java.text.MessageFormat;

/*
создает все таблицыб типыб кодеки и пространство ключей
*/
public final class CassandraTablesAndTypesRegister extends CassandraConverter {
    private final Session session;

    private Session getSession() {
        return this.session;
    }

    public static CassandraTablesAndTypesRegister generate ( final Session session ) {
        return new CassandraTablesAndTypesRegister( session );
    }

    private CassandraTablesAndTypesRegister( final Session session ) {
        this.session = session;

        this.createAllKeyspace();
        this.createAllTables();

        super.logging( "All tables, keyspace and types were created" );
    }

    /*
    Хранит все данные для создания новой таблицы в БД
    */
    private static class TableRegistration extends CassandraConverter {
        public CassandraTables getTableName() {
            return this.tableName;
        }

        public CassandraTables getKeyspace() {
            return this.keyspace;
        }

        public String getConvertedValue() {
            return convertedValue;
        }

        public String getPrefix() {
            return this.prefix;
        }

        /*
        навзвание таблицы
         */
        private final CassandraTables tableName;
        /*
        навзвание пространства в котором находиться таблица
        */
        private final CassandraTables keyspace;

        /*
        сконвертированное значение объекта в CQL понятный язык
        */
        private final String convertedValue;
        /*
        хранит дом значение для CQL
        в особенности Primary key
         */
        private final String prefix;

        public static TableRegistration from (
                final CassandraTables keyspace,
                final CassandraTables tableName,
                final Class object,
                final String prefix
        ) {
            return new TableRegistration( keyspace, tableName, object, prefix );
        }

        private TableRegistration (
                final CassandraTables keyspace,
                final CassandraTables tableName,
                final Class object,
                final String prefix
        ) {
            this.convertedValue = super.convertClassToCassandra.apply( object );
            this.tableName = tableName;
            this.keyspace = keyspace;
            this.prefix = prefix;
        }
    }

    /*
    Хранит все данные для создания нового пространства в БД
    */
    private static class KeyspaceRegistration {
        public String getPrefix() {
            return this.prefix;
        }

        /*
        хранит CQL команду для оздания нового пространства
        */
        private final String prefix;

        public static KeyspaceRegistration from (
                final CassandraTables keyspace
        ) {
            return new KeyspaceRegistration( keyspace );
        }

        private KeyspaceRegistration (
                final CassandraTables keyspace
        ) {
            this.prefix = String.format(
                    """
                    %s %s %s
                    WITH REPLICATION = {
                        'class' : 'SimpleStrategy',
                        'replication_factor': 1 }
                    AND DURABLE_WRITES = false;
                    """,
                    CassandraCommands.CREATE_KEYSPACE,
                    CassandraCommands.IF_NOT_EXISTS.replaceAll( ";", "" ),
                    keyspace );
        }
    }

    private void createAllKeyspace () {
        /*
        создаем все пространства в БД
         */
        this.createKeyspace( KeyspaceRegistration.from( CassandraTables.TRACKERS ) );
        this.createKeyspace( KeyspaceRegistration.from( CassandraTables.ESCORT ) );
    }

    private void createAllTables () {
        this.getSession().execute(
                MessageFormat.format(
                        """
                        {0} {1}.{2}
                        ( trackersId {3} PRIMARY KEY,
                        patrulPassportSeries {3},
                        gosnumber {3},
                        policeType {3},
                        status {4},
                        latitude {5},
                        longitude {5},
                        totalActivityTime {5},
                        lastActiveDate {6},
                        dateOfRegistration {6} );
                        """,
                        CassandraCommands.CREATE_TABLE,
                        CassandraTables.TRACKERS,
                        CassandraTables.TRACKERSID,
                        CassandraDataTypes.TEXT,
                        CassandraDataTypes.BOOLEAN,
                        CassandraDataTypes.DOUBLE,
                        CassandraDataTypes.TIMESTAMP
                )
        );

        this.getSession().execute (
                MessageFormat.format(
                        """
                                {0} {1}.{2}
                                ( imei {3},
                                date {4},
                                speed {5},
                                latitude {5},
                                longitude {5},
                                address {3},
                                PRIMARY KEY ( (imei), date ) ) {6}
                        """,
                        CassandraCommands.CREATE_TABLE,
                        CassandraTables.TRACKERS,
                        CassandraTables.TRACKERS_LOCATION_TABLE,
                        CassandraDataTypes.TEXT,
                        CassandraDataTypes.TIMESTAMP,
                        CassandraDataTypes.DOUBLE,
                        CassandraFunctions.WITH_CLUSTERING_ORDER.formatted( "date" )
                )
        );

        this.getSession().execute (
                MessageFormat.format(
                        """
                                {0} {1}.{2}
                                ( imei {3},
                                date {4},
                                speed {5},
                                distance {5},
                                PRIMARY KEY ( (imei), date ) ) {6}
                        """,
                        CassandraCommands.CREATE_TABLE,
                        CassandraTables.TRACKERS,
                        CassandraTables.TRACKER_FUEL_CONSUMPTION,
                        CassandraDataTypes.TEXT,
                        CassandraDataTypes.TIMESTAMP,
                        CassandraDataTypes.DOUBLE,
                        CassandraFunctions.WITH_CLUSTERING_ORDER.formatted( "date" )
                )
        );

        this.createTable( TableRegistration.from(
                CassandraTables.ESCORT,
                CassandraTables.TUPLE_OF_CAR,
                TupleOfCar.class,
                        """
                        , PRIMARY KEY( ( uuid ), trackerId ) ) %s
                        """.formatted(
                                CassandraFunctions.WITH_CLUSTERING_ORDER.formatted( "trackerId" )
                        )
        ) );

        this.getSession().execute(
                MessageFormat.format(
                        """
                        {0} {1}.{2} {3}
                        """,
                        CassandraCommands.CREATE_INDEX,
                        CassandraTables.ESCORT,
                        CassandraTables.TUPLE_OF_CAR,
                        "( trackerId );"
                )
        );

        this.getSession().execute (
                MessageFormat.format(
                        """
                                {0} {1}.{2}
                                ( trackersId {3}, -- IMEI трекера
                                patrulPassportSeries {3}, -- серия паспорта патрульного привязанного к машине
                                gosnumber {3}, -- номер машины
                                status {4}, -- показывает были ли активна машина в течении последних 30 секунд
                                latitude {5}, -- локация машины
                                longitude {5}, -- локация машины
                                totalActivityTime {5}, -- общее количество времени активности машиныы в секундах
                                lastActiveDate {6}, -- время последнего приема сигнала от трекера
                                dateOfRegistration {6}, -- дата регистрации трекера
                                PRIMARY KEY ( (trackersId), lastActiveDate ) ) {7}
                        """,
                        CassandraCommands.CREATE_TABLE,
                        CassandraTables.ESCORT,
                        CassandraTables.TRACKERSID,
                        CassandraDataTypes.TEXT,
                        CassandraDataTypes.BOOLEAN,
                        CassandraDataTypes.DOUBLE,
                        CassandraDataTypes.TIMESTAMP,
                        CassandraFunctions.WITH_CLUSTERING_ORDER.formatted( "lastActiveDate ASC, trackersId" )
                )
        );

        this.getSession().execute (
                MessageFormat.format(
                        """
                                {0} {1}.{2}
                                ( imei {3},
                                 date {4},
                                 speed {5},
                                 altitude {5},
                                 longitude {5},
                                 address {3},
                                 PRIMARY KEY ( (imei), date ) ) {6}
                        """,
                        CassandraCommands.CREATE_TABLE,
                        CassandraTables.ESCORT,
                        CassandraTables.ESCORT_LOCATION,
                        CassandraDataTypes.TEXT,
                        CassandraDataTypes.TIMESTAMP,
                        CassandraDataTypes.DOUBLE,
                        CassandraFunctions.WITH_CLUSTERING_ORDER.formatted( "date ASC, imei" )
                )
        );
    }

    /*
    функция создает новые таблицы в БД
    */
    private void createTable (
            final TableRegistration tableRegistration
    ) {
        this.getSession().execute(
                MessageFormat.format(
                        """
                        {0} {1}.{2} {3} {4}
                        """,
                        CassandraCommands.CREATE_TABLE,
                        tableRegistration.getKeyspace(),
                        tableRegistration.getTableName(),
                        tableRegistration.getConvertedValue(),
                        tableRegistration.getPrefix() ) );
    }

    /*
    функция создает новые пространства в БД
    */
    private void createKeyspace (
            final KeyspaceRegistration keyspaceRegistration
    ) {
        this.getSession().execute( keyspaceRegistration.getPrefix() );
    }
}
