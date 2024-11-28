package com.ssd.mvd.interfaces.entity;

import com.datastax.driver.core.BatchStatement;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.delete.DeleteSelection;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.insert.InsertInto;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.select.SelectFrom;
import com.datastax.oss.driver.api.querybuilder.update.Update;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;

import com.datastax.oss.driver.api.querybuilder.update.UpdateStart;
import com.ssd.mvd.constants.Errors;
import com.ssd.mvd.inspectors.AnnotationInspector;
import com.ssd.mvd.inspectors.dataTypesInpectors.StringOperations;
import com.ssd.mvd.interfaces.ServiceCommonMethods;
import com.ssd.mvd.database.CassandraDataControl;

import com.ssd.mvd.constants.cassandra.CassandraCommands;
import com.ssd.mvd.constants.cassandra.CassandraTables;

public interface EntityToCassandraConverter extends ServiceCommonMethods {
    default int getParallelNumber () {
        return Math.abs(
                this.getEntityKeyspaceName().name().length() + this.getEntityTableName().name().length()
        );
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
    default UpdateStart startUpdate () {
        return QueryBuilder.update(
                CqlIdentifier.fromCql( this.getEntityKeyspaceName().name() ),
                CqlIdentifier.fromCql( this.getEntityTableName().name() )
        );
    }

    @lombok.NonNull
    default InsertInto startInsert () {
        return QueryBuilder.insertInto(
                this.getEntityKeyspaceName().name(),
                this.getEntityTableName().name()
        );
    }

    @lombok.NonNull
    default SelectFrom startSelect () {
        return QueryBuilder.selectFrom(
                this.getEntityKeyspaceName().name(),
                this.getEntityTableName().name()
        );
    }

    @lombok.NonNull
    default DeleteSelection startDelete () {
        return QueryBuilder.deleteFrom(
                this.getEntityKeyspaceName().name(),
                this.getEntityTableName().name()
        );
    }

    @lombok.NonNull
    default Insert getEntityInsert () {
        throw new IllegalArgumentException(
                Errors.METHOD_NOT_REALIZED.translate(
                        "getEntityInsert",
                        this.getClass().getName()
                )
        );
    }

    @lombok.NonNull
    default Update getEntityUpdate () {
        throw new IllegalArgumentException(
                Errors.METHOD_NOT_REALIZED.translate(
                        "getEntityUpdate",
                        this.getClass().getName()
                )
        );
    }

    @lombok.NonNull
    default Delete getEntityDelete () {
        throw new IllegalArgumentException(
                Errors.METHOD_NOT_REALIZED.translate(
                        "getEntityDelete",
                        this.getClass().getName()
                )
        );
    }

    @lombok.NonNull
    @org.jetbrains.annotations.Contract( value = "_ -> !null" )
    default Select getEntitySelect (
            final Object ... params
    ) {
        return this.startSelect().all();
    }

    @SuppressWarnings(
            value = "используются для сушностей которые связаны с многими другими"
    )
    @lombok.NonNull
    default BatchStatement getEntityInsertBatch () {
        throw new IllegalArgumentException(
                Errors.METHOD_NOT_REALIZED.translate(
                        "getEntityInsertBatch",
                        this.getClass().getName()
                )
        );
    }

    @lombok.NonNull
    default CassandraTables getEntityTableName () {
        return AnnotationInspector.getEntityKeyspaceOrTableName(
                this,
                false
        );
    }

    @lombok.NonNull
    default CassandraTables getEntityKeyspaceName () {
        return AnnotationInspector.getEntityKeyspaceOrTableName(
                this,
                true
        );
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
