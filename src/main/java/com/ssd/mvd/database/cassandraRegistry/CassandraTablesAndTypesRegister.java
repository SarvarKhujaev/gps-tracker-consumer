package com.ssd.mvd.database.cassandraRegistry;

import com.datastax.driver.core.Session;
import com.ssd.mvd.constants.CassandraCommands;
import com.ssd.mvd.constants.CassandraTables;
import com.ssd.mvd.interfaces.DatabaseCommonMethods;

/*
создает все таблицыб типыб кодеки и пространство ключей
*/
public final class CassandraTablesAndTypesRegister extends CassandraConverter implements DatabaseCommonMethods {
    public static CassandraTablesAndTypesRegister generate (
            final Session session
    ) {
        return new CassandraTablesAndTypesRegister();
    }

    private CassandraTablesAndTypesRegister() {
        this.createAllKeyspace();
        CassandraTableRegistration.generate();

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
                    keyspace
            );
        }
    }

    private void createAllKeyspace () {
        /*
        создаем все пространства в БД
        */
        this.getSession().execute( KeyspaceRegistration.from( CassandraTables.TRACKERS ).getPrefix() );
        this.getSession().execute( KeyspaceRegistration.from( CassandraTables.ESCORT ).getPrefix() );
    }
}
