package com.ssd.mvd.interfaces;

import com.ssd.mvd.database.CassandraDataControl;
import com.ssd.mvd.constants.CassandraCommands;

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
