package com.ssd.mvd.interfaces;

import com.ssd.mvd.constants.CassandraCommands;
import com.ssd.mvd.constants.CassandraTables;
import com.ssd.mvd.database.CassandraDataControl;

import java.text.MessageFormat;

public interface EntityToCassandraConverter {
    default String getEntityInsertCommand () {
        return CassandraCommands.INSERT_INTO;
    };

    default String getEntityDeleteCommand () {
        return CassandraCommands.DELETE;
    }

    default String getEntityUpdateCommand () {
        return CassandraCommands.UPDATE;
    }

    default String getSelectAllCommand () {
        return MessageFormat.format(
                """
                {0} {1}.{2};
                """,
                CassandraCommands.SELECT_ALL,

                this.getEntityKeyspaceName(),
                this.getEntityTableName()
        );
    }

    default CassandraTables getEntityTableName () {
        return CassandraTables.TRACKERS_LOCATION_TABLE;
    }

    default CassandraTables getEntityKeyspaceName () {
        return CassandraTables.TRACKERS;
    }

    default int getParallelNumber () {
        return Math.abs(
                this.getEntityKeyspaceName().name().length() + this.getEntityTableName().name().length()
        );
    }

    /*
    сохраняет любой объект
    */
    default boolean save () {
        return CassandraDataControl
                .getInstance()
                .getSession()
                .execute( this.getEntityInsertCommand() )
                .wasApplied();
    }

    default boolean delete () {
        return CassandraDataControl
                .getInstance()
                .getSession()
                .execute( this.getEntityDeleteCommand() )
                .wasApplied();
    }

    default boolean updateEntity() {
        return CassandraDataControl
                .getInstance()
                .getSession()
                .execute( this.getEntityUpdateCommand() )
                .wasApplied();
    }
}
