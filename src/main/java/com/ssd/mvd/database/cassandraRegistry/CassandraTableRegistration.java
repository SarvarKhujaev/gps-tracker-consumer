package com.ssd.mvd.database.cassandraRegistry;

import java.text.MessageFormat;

import com.ssd.mvd.constants.*;
import com.ssd.mvd.entity.TupleOfCar;

import com.ssd.mvd.inspectors.TimeInspector;
import com.ssd.mvd.inspectors.StringOperations;
import com.ssd.mvd.inspectors.CassandraConverter;

import com.ssd.mvd.interfaces.DatabaseCommonMethods;
import com.ssd.mvd.interfaces.EntityToCassandraConverter;

@com.ssd.mvd.annotations.ImmutableEntityAnnotation
public final class CassandraTableRegistration extends CassandraTablesAndTypesRegister implements DatabaseCommonMethods {
    public static void generate () {
        new CassandraTableRegistration();
    }

    public CassandraTableRegistration() {
        super();
        this.createAllTables();
    }

    @SuppressWarnings(
            value = "Хранит все данные для создания новой таблицы в БД"
    )
    @com.ssd.mvd.annotations.ImmutableEntityAnnotation
    public static class TableRegistration extends CassandraConverter {
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
                @lombok.NonNull final CassandraTables keyspace,
                @lombok.NonNull final CassandraTables tableName,
                @lombok.NonNull final Class<? extends EntityToCassandraConverter> object,
                @lombok.NonNull final String prefix
        ) {
            return new TableRegistration( keyspace, tableName, object, prefix );
        }

        private TableRegistration (
                final CassandraTables keyspace,
                final CassandraTables tableName,
                final Class<? extends EntityToCassandraConverter> object,
                final String prefix
        ) {
            super( TableRegistration.class );
            this.convertedValue = super.convertClassToCassandra( object );
            this.tableName = tableName;
            this.keyspace = keyspace;
            this.prefix = prefix;
        }
    }

    private void createAllTables () {
        this.completeCommand(
                MessageFormat.format(
                        """
                        {0} {1}.{2}
                        (
                            trackersId              {3},
                            patrulPassportSeries    {3},
                            gosnumber               {3},
                            policeType              {3},
                            status                  {4},
                            latitude                {5},
                            longitude               {5},
                            totalActivityTime       {5},
                            lastActiveDate          {6},
                            dateOfRegistration      {6},
                            PRIMARY KEY ( (trackersId), lastActiveDate )
                        ) {7};
                        """,
                        CassandraCommands.CREATE_TABLE,

                        CassandraTables.TRACKERS,
                        CassandraTables.TRACKERSID,

                        CassandraDataTypes.TEXT,
                        CassandraDataTypes.BOOLEAN,
                        CassandraDataTypes.DOUBLE,
                        CassandraDataTypes.TIMESTAMP,

                        String.join(
                                " AND ",
                                CassandraCommands.WITH_CLUSTERING_ORDER.formatted( "lastActiveDate ASC" ),
                                CassandraCommands.WITH_COMPACTION.formatted( CassandraCompactionTypes.LEVELED_COMPACTION ),
                                CassandraCommands.WITH_COMMENT.formatted(
                                        """
                                        хранит данные, о том как долго патрульный пользовался планшетом
                                        """
                                ),
                                CassandraCommands.WITH_COMPRESSION.formatted( CassandraCompressionTypes.LZ4, 64 ),
                                CassandraCommands.WITH_TTL.formatted( TimeInspector.DAY_IN_SECOND * 7 ),
                                StringOperations.generateID()
                        )
                )
        );

        this.completeCommand (
                MessageFormat.format(
                        """
                        {0} {1}.{2}
                        (
                            imei        {3},
                            date        {4},
                            speed       {5},
                            latitude    {5},
                            longitude   {5},
                            address     {3},
                            PRIMARY KEY ( (imei), date )
                        )
                        {6};
                        """,
                        CassandraCommands.CREATE_TABLE,

                        CassandraTables.TRACKERS,
                        CassandraTables.TRACKERS_LOCATION_TABLE,

                        CassandraDataTypes.TEXT,
                        CassandraDataTypes.TIMESTAMP,
                        CassandraDataTypes.DOUBLE,

                        String.join(
                                " AND ",
                                CassandraCommands.WITH_CLUSTERING_ORDER.formatted( "date ASC" ),
                                CassandraCommands.WITH_COMPACTION.formatted( CassandraCompactionTypes.TIME_WINDOW_COMPACTION ),
                                CassandraCommands.WITH_COMMENT.formatted(
                                        """
                                        хранит данные, о том как долго патрульный пользовался планшетом
                                        """
                                ),
                                CassandraCommands.WITH_COMPRESSION.formatted( CassandraCompressionTypes.LZ4, 64 ),
                                CassandraCommands.WITH_TTL.formatted( TimeInspector.DAY_IN_SECOND * 60 ),
                                StringOperations.generateID()
                        )
                )
        );

        this.completeCommand (
                MessageFormat.format(
                        """
                        {0} {1}.{2}
                        (
                            imei {3},
                            date {4},
                            speed {5},
                            distance {5},
                            PRIMARY KEY ( (imei), date )
                        ) {6};
                        """,
                        CassandraCommands.CREATE_TABLE,

                        CassandraTables.TRACKERS,
                        CassandraTables.TRACKER_FUEL_CONSUMPTION,

                        CassandraDataTypes.TEXT,
                        CassandraDataTypes.TIMESTAMP,
                        CassandraDataTypes.DOUBLE,

                        String.join(
                                " AND ",
                                CassandraCommands.WITH_CLUSTERING_ORDER.formatted( "date ASC" ),
                                CassandraCommands.WITH_COMPACTION.formatted( CassandraCompactionTypes.LEVELED_COMPACTION ),
                                CassandraCommands.WITH_COMMENT.formatted(
                                        """
                                        хранит данные, о том как долго патрульный пользовался планшетом
                                        """
                                ),
                                CassandraCommands.WITH_COMPRESSION.formatted( CassandraCompressionTypes.LZ4, 64 ),
                                CassandraCommands.WITH_TTL.formatted( TimeInspector.DAY_IN_SECOND * 70 ),
                                StringOperations.generateID()
                        )
                )
        );

        this.createTable(
                TableRegistration.from(
                        CassandraTables.ESCORT,
                        CassandraTables.TUPLE_OF_CAR,
                        TupleOfCar.class,
                        """
                        , PRIMARY KEY( ( uuid ), trackerId ) ) %s AND %s AND %s AND %s AND %s AND %s;
                        """.formatted(
                                CassandraCommands.WITH_CLUSTERING_ORDER.formatted( "trackerId ASC" ),
                                CassandraCommands.WITH_COMPACTION.formatted( CassandraCompactionTypes.LEVELED_COMPACTION ),
                                CassandraCommands.WITH_COMMENT.formatted(
                                        """
                                        хранит данные, о том как долго патрульный пользовался планшетом
                                        """
                                ),
                                CassandraCommands.WITH_COMPRESSION.formatted( CassandraCompressionTypes.LZ4, 64 ),
                                CassandraCommands.WITH_TTL.formatted( TimeInspector.DAY_IN_SECOND * 70 ),
                                StringOperations.generateID()
                        )
                )
        );

        this.completeCommand(
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

        this.completeCommand (
                MessageFormat.format(
                        """
                        {0} {1}.{2}
                        (
                            trackersId              {3}, -- IMEI трекера
                            patrulPassportSeries    {3}, -- серия паспорта патрульного привязанного к машине
                            gosnumber               {3}, -- номер машины
                            status                  {4}, -- показывает были ли активна машина в течении последних 30 секунд
                            latitude                {5}, -- локация машины
                            longitude               {5}, -- локация машины
                            totalActivityTime       {5}, -- общее количество времени активности машиныы в секундах
                            lastActiveDate          {6}, -- время последнего приема сигнала от трекера
                            dateOfRegistration      {6}, -- дата регистрации трекера
                            PRIMARY KEY ( (trackersId), lastActiveDate )
                        )
                        {7}
                        """,
                        CassandraCommands.CREATE_TABLE,

                        CassandraTables.ESCORT,
                        CassandraTables.TRACKERSID,

                        CassandraDataTypes.TEXT,
                        CassandraDataTypes.BOOLEAN,
                        CassandraDataTypes.DOUBLE,
                        CassandraDataTypes.TIMESTAMP,

                        String.join(
                                " AND ",
                                CassandraCommands.WITH_CLUSTERING_ORDER.formatted( "lastActiveDate ASC" ),
                                CassandraCommands.WITH_COMPACTION.formatted( CassandraCompactionTypes.LEVELED_COMPACTION ),
                                CassandraCommands.WITH_COMMENT.formatted(
                                        """
                                        хранит данные, о том как долго патрульный пользовался планшетом
                                        """
                                ),
                                CassandraCommands.WITH_COMPRESSION.formatted( CassandraCompressionTypes.LZ4, 64 ),
                                CassandraCommands.WITH_TTL.formatted( TimeInspector.DAY_IN_SECOND * 70 ),
                                StringOperations.generateID()
                        )
                )
        );

        this.completeCommand (
                MessageFormat.format(
                        """
                        {0} {1}.{2}
                        (
                        imei            {3},
                        date            {4},
                        speed           {5},
                        altitude        {5},
                        longitude       {5},
                        address         {3},
                        PRIMARY KEY ( (imei), date ) )
                        {6}
                        """,
                        CassandraCommands.CREATE_TABLE,

                        CassandraTables.ESCORT,
                        CassandraTables.ESCORT_LOCATION,

                        CassandraDataTypes.TEXT,
                        CassandraDataTypes.TIMESTAMP,
                        CassandraDataTypes.DOUBLE,

                        String.join(
                                " AND ",
                                CassandraCommands.WITH_CLUSTERING_ORDER.formatted( "date ASC" ),
                                CassandraCommands.WITH_COMPACTION.formatted( CassandraCompactionTypes.LEVELED_COMPACTION ),
                                CassandraCommands.WITH_COMMENT.formatted(
                                        """
                                        хранит данные, о том как долго патрульный пользовался планшетом
                                        """
                                ),
                                CassandraCommands.WITH_COMPRESSION.formatted( CassandraCompressionTypes.LZ4, 64 ),
                                CassandraCommands.WITH_TTL.formatted( TimeInspector.DAY_IN_SECOND * 7 ),
                                StringOperations.generateID()
                        )
                )
        );
    }

    @SuppressWarnings( value = "функция создает новые таблицы в БД" )
    private void createTable (
            @lombok.NonNull final TableRegistration tableRegistration
    ) {
        this.completeCommand(
                MessageFormat.format(
                        """
                        {0} {1}.{2} {3} {4}
                        """,
                        CassandraCommands.CREATE_TABLE,

                        tableRegistration.getKeyspace(),
                        tableRegistration.getTableName(),
                        tableRegistration.getConvertedValue(),
                        tableRegistration.getPrefix()
                )
        );
    }
}
