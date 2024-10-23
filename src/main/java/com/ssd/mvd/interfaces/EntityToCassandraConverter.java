package com.ssd.mvd.interfaces;

import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.update.Update;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;

import com.ssd.mvd.database.CassandraDataControl;
import com.ssd.mvd.inspectors.StringOperations;

import com.ssd.mvd.constants.CassandraCommands;
import com.ssd.mvd.constants.CassandraTables;

public interface EntityToCassandraConverter extends ServiceCommonMethods {
    default int getParallelNumber () {
        return Math.abs(
                this.getEntityKeyspaceName().name().length() + this.getEntityTableName().name().length()
        );
    }

    @lombok.NonNull
    default Insert getEntityInsert () {
        return null;
    }

    @lombok.NonNull
    default Update getEntityUpdate () {
        return null;
    }

    @lombok.NonNull
    default Delete getEntityDelete () {
        return null;
    }

    @lombok.NonNull
    @org.jetbrains.annotations.Contract( value = "_ -> !null" )
    default Select getEntitySelect (
            final Object ... params
    ) {
        return QueryBuilder.selectFrom(
                this.getEntityKeyspaceName().name(),
                this.getEntityTableName().name()
        ).all();
    }

    @lombok.NonNull
    default String getEntityInsertCommand () {
        return CassandraCommands.INSERT_INTO;
    }

    @lombok.NonNull
    default String getEntityDeleteCommand () {
        return CassandraCommands.DELETE;
    }

    @lombok.NonNull
    default String getEntityUpdateCommand () {
        return CassandraCommands.UPDATE;
    }

    @lombok.NonNull
    default CassandraTables getEntityTableName () {
        return CassandraTables.TRACKERS_LOCATION_TABLE;
    }

    @lombok.NonNull
    default CassandraTables getEntityKeyspaceName () {
        return CassandraTables.TABLETS;
    }

    @lombok.NonNull
    default String getCreateTableOptions () {
        return StringOperations.generateID();
    }

    @SuppressWarnings( value = "сохраняет любой объект" )
    default boolean save () {
        return CassandraDataControl
                .getInstance()
                .completeCommand( this.getEntityInsert() )
                .wasApplied();
    }

    default boolean delete () {
        return CassandraDataControl
                .getInstance()
                .completeCommand( this.getEntityDelete() )
                .wasApplied();
    }

    default boolean updateEntity() {
        return CassandraDataControl
                .getInstance()
                .completeCommand( this.getEntityUpdate() )
                .wasApplied();
    }
}
