package com.ssd.mvd.database;

import com.datastax.driver.core.Session;
import com.ssd.mvd.constants.CassandraTables;
import com.ssd.mvd.constants.CassandraCommands;
import com.ssd.mvd.database.cassandraRegistry.CassandraTableRegistration;

/*
создает все таблицыб типыб кодеки и пространство ключей
*/
public final class CassandraTablesAndTypesRegister extends CassandraConverter {
    private final Session session;

    private Session getSession() {
        return this.session;
    }

    public static CassandraTablesAndTypesRegister generate (
            final Session session
    ) {
        return new CassandraTablesAndTypesRegister( session );
    }

    private CassandraTablesAndTypesRegister( final Session session ) {
        this.session = session;

        this.createAllKeyspace();
        CassandraTableRegistration.generate( this.session );

        super.logging( "All tables, keyspace and types were created" );
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

    /*
    функция создает новые пространства в БД
    */
    private void createKeyspace (
            final KeyspaceRegistration keyspaceRegistration
    ) {
        this.getSession().execute( keyspaceRegistration.getPrefix() );
    }
}
