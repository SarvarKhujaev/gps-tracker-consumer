package com.ssd.mvd.database.cassandraRegistry;

import java.text.MessageFormat;

import com.ssd.mvd.constants.*;
import com.ssd.mvd.entity.TupleOfCar;
import com.ssd.mvd.inspectors.TimeInspector;
import com.ssd.mvd.inspectors.StringOperations;
import com.ssd.mvd.interfaces.DatabaseCommonMethods;

public final class CassandraTableRegistration extends StringOperations implements DatabaseCommonMethods {
    public static void generate () {
        new CassandraTableRegistration();
    }

    public CassandraTableRegistration() {
        this.createAllTables();
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
                final Class<?> object,
                final String prefix
        ) {
            return new TableRegistration( keyspace, tableName, object, prefix );
        }

        private TableRegistration (
                final CassandraTables keyspace,
                final CassandraTables tableName,
                final Class<?> object,
                final String prefix
        ) {
            this.convertedValue = super.convertClassToCassandra.apply( object );
            this.tableName = tableName;
            this.keyspace = keyspace;
            this.prefix = prefix;
        }
    }

    private void createAllTables () {
        this.getSession().execute(
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
                        dateOfRegistration      {6}
                        )  PRIMARY KEY ( trackersId ) WITH %s AND %s AND %s AND %s AND %s AND %s;
                        """.formatted(
                                CassandraCommands.WITH_CLUSTERING_ORDER.formatted( "simCardNumber ASC" ),
                                CassandraCommands.WITH_COMPACTION.formatted( CassandraCompactionTypes.LEVELED_COMPACTION ),
                                CassandraCommands.WITH_COMMENT.formatted(
                                        """
                                        хранит данные, о том как долго патрульный пользовался планшетом
                                        """
                                ),
                                CassandraCommands.WITH_COMPRESSION.formatted( CassandraCompressionTypes.LZ4, 64 ),
                                CassandraCommands.WITH_TTL.formatted( TimeInspector.DAY_IN_SECOND * 7 ),
                                super.generateID()
                        ),
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
                        (
                        imei        {3},
                        date        {4},
                        speed       {5},
                        latitude    {5},
                        longitude   {5},
                        address     {3},
                        PRIMARY KEY ( (imei), date ) )
                        WITH %s AND %s AND %s AND %s AND %s AND %s;
                        """.formatted(
                                CassandraCommands.WITH_CLUSTERING_ORDER.formatted( "date ASC" ),
                                CassandraCommands.WITH_COMPACTION.formatted( CassandraCompactionTypes.TIME_WINDOW_COMPACTION ),
                                CassandraCommands.WITH_COMMENT.formatted(
                                        """
                                        хранит данные, о том как долго патрульный пользовался планшетом
                                        """
                                ),
                                CassandraCommands.WITH_COMPRESSION.formatted( CassandraCompressionTypes.LZ4, 64 ),
                                CassandraCommands.WITH_TTL.formatted( TimeInspector.DAY_IN_SECOND * 60 ),
                                super.generateID()
                        ),
                        CassandraCommands.CREATE_TABLE,

                        CassandraTables.TRACKERS,
                        CassandraTables.TRACKERS_LOCATION_TABLE,

                        CassandraDataTypes.TEXT,
                        CassandraDataTypes.TIMESTAMP,
                        CassandraDataTypes.DOUBLE
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
                                PRIMARY KEY ( (imei), date ) ) WITH %s AND %s AND %s AND %s AND %s AND %s;
                        """.formatted(
                                CassandraCommands.WITH_CLUSTERING_ORDER.formatted( "date ASC" ),
                                CassandraCommands.WITH_COMPACTION.formatted( CassandraCompactionTypes.LEVELED_COMPACTION ),
                                CassandraCommands.WITH_COMMENT.formatted(
                                        """
                                        хранит данные, о том как долго патрульный пользовался планшетом
                                        """
                                ),
                                CassandraCommands.WITH_COMPRESSION.formatted( CassandraCompressionTypes.LZ4, 64 ),
                                CassandraCommands.WITH_TTL.formatted( TimeInspector.DAY_IN_SECOND * 70 ),
                                super.generateID()
                        ),
                        CassandraCommands.CREATE_TABLE,

                        CassandraTables.TRACKERS,
                        CassandraTables.TRACKER_FUEL_CONSUMPTION,

                        CassandraDataTypes.TEXT,
                        CassandraDataTypes.TIMESTAMP,
                        CassandraDataTypes.DOUBLE
                )
        );

        this.createTable(
                TableRegistration.from(
                        CassandraTables.ESCORT,
                        CassandraTables.TUPLE_OF_CAR,
                        TupleOfCar.class,
                        """
                        , PRIMARY KEY( ( uuid ), trackerId ) ) WITH %s AND %s AND %s AND %s AND %s AND %s;
                        """.formatted(
                                CassandraCommands.WITH_CLUSTERING_ORDER.formatted( "trackerId ASC" ),
                                CassandraCommands.WITH_COMPACTION.formatted( CassandraCompactionTypes.COMPACT_STORAGE ),
                                CassandraCommands.WITH_COMMENT.formatted(
                                        """
                                        хранит данные, о том как долго патрульный пользовался планшетом
                                        """
                                ),
                                CassandraCommands.WITH_COMPRESSION.formatted( CassandraCompressionTypes.LZ4, 64 ),
                                CassandraCommands.WITH_TTL.formatted( TimeInspector.DAY_IN_SECOND * 70 ),
                                super.generateID()
                        )
                )
        );

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
                        PRIMARY KEY ( (trackersId), lastActiveDate ) )
                        WITH %s AND %s AND %s AND %s AND %s AND %s;
                        """.formatted(
                                CassandraCommands.WITH_CLUSTERING_ORDER.formatted( "lastActiveDate ASC, trackersId" ),
                                CassandraCommands.WITH_COMPACTION.formatted( CassandraCompactionTypes.LEVELED_COMPACTION ),
                                CassandraCommands.WITH_COMMENT.formatted(
                                        """
                                        хранит данные, о том как долго патрульный пользовался планшетом
                                        """
                                ),
                                CassandraCommands.WITH_COMPRESSION.formatted( CassandraCompressionTypes.LZ4, 64 ),
                                CassandraCommands.WITH_TTL.formatted( TimeInspector.DAY_IN_SECOND * 70 ),
                                super.generateID()
                        ),
                        CassandraCommands.CREATE_TABLE,

                        CassandraTables.ESCORT,
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
                        (
                        imei            {3},
                        date            {4},
                        speed           {5},
                        altitude        {5},
                        longitude       {5},
                        address         {3},
                        PRIMARY KEY ( (imei), date ) )
                        WITH %s AND %s AND %s AND %s AND %s AND %s;
                        """.formatted(
                                CassandraCommands.WITH_CLUSTERING_ORDER.formatted( "date ASC, imei" ),
                                CassandraCommands.WITH_COMPACTION.formatted( CassandraCompactionTypes.LEVELED_COMPACTION ),
                                CassandraCommands.WITH_COMMENT.formatted(
                                        """
                                        хранит данные, о том как долго патрульный пользовался планшетом
                                        """
                                ),
                                CassandraCommands.WITH_COMPRESSION.formatted( CassandraCompressionTypes.LZ4, 64 ),
                                CassandraCommands.WITH_TTL.formatted( TimeInspector.DAY_IN_SECOND * 7 ),
                                super.generateID()
                        ),
                        CassandraCommands.CREATE_TABLE,

                        CassandraTables.ESCORT,
                        CassandraTables.ESCORT_LOCATION,

                        CassandraDataTypes.TEXT,
                        CassandraDataTypes.TIMESTAMP,
                        CassandraDataTypes.DOUBLE
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
                        tableRegistration.getPrefix()
                )
        );
    }
}
