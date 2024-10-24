package com.ssd.mvd.interfaces;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.BuildableQuery;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.driver.core.*;

import com.ssd.mvd.inspectors.AnnotationInspector;
import com.ssd.mvd.database.CassandraDataControl;
import com.ssd.mvd.inspectors.EntitiesInstances;
import com.ssd.mvd.inspectors.StringOperations;
import org.apache.commons.lang3.Validate;

import java.lang.ref.WeakReference;
import java.util.List;

@SuppressWarnings(
        value = "хранит все стандартные методы для сервисов работающих с БД"
)
public interface DatabaseCommonMethods extends ServiceCommonMethods {
    default void checkSessionNotClosed () {
        Validate.isTrue(
                !this.getSession().isClosed(),
                String.join(
                        StringOperations.SPACE,
                        CassandraDataControl.class.getName(),
                        "is closed"
                )
        );
    }

    @SuppressWarnings( value = "возвращает одно конкретное значение из БД" )
    @lombok.NonNull
    @org.jetbrains.annotations.Contract( value = "_, _, _ -> !null" )
    default <U> WeakReference< GettableData > getRowFromTabletsKeyspace (
            // название таблицы внутри Tablets
            @lombok.NonNull @com.typesafe.config.Optional final EntityToCassandraConverter entityToCassandraConverter,
            // название колонки
            @lombok.NonNull final String columnName,
            // параметр по которому введется поиск
            @lombok.NonNull final @com.typesafe.config.Optional U paramName
    ) {
        this.checkSessionNotClosed();

        return EntitiesInstances.generateWeakEntity(
                this.completeCommand(
                        QueryBuilder.selectFrom(
                                        entityToCassandraConverter.getEntityKeyspaceName().name(),
                                        entityToCassandraConverter.getEntityTableName().name()
                                ).all()
                                .where( Relation.column( CqlIdentifier.fromCql( columnName ) ).isEqualTo( literal( paramName ) ) )
                ).one()
        );
    }

    @SuppressWarnings( value = "возвращает одно конкретное значение из БД" )
    @lombok.NonNull
    @org.jetbrains.annotations.Contract( value = "_, _ -> fail" )
    default <U> WeakReference< com.datastax.driver.core.GettableData > getRowFromTabletsKeyspace (
            // название таблицы внутри Tablets
            @lombok.NonNull @com.typesafe.config.Optional final EntityToCassandraConverter entityToCassandraConverter,
            // параметр по которому введется поиск
            @lombok.NonNull final @com.typesafe.config.Optional U paramName
    ) {
        this.checkSessionNotClosed();

        return EntitiesInstances.generateWeakEntity(
                this.completeCommand(
                        QueryBuilder.selectFrom(
                                        entityToCassandraConverter.getEntityKeyspaceName().name(),
                                        entityToCassandraConverter.getEntityTableName().name()
                                ).all()
                                .where(
                                        Relation.column(
                                                CqlIdentifier.fromCql( AnnotationInspector.getEntityPrimaryKey( entityToCassandraConverter )[0] )
                                        ).isEqualTo( literal( paramName ) )
                                )
                ).one()
        );
    }

    @lombok.NonNull
    @org.jetbrains.annotations.Contract( value = "_, _, _ -> fail" )
    default <T, U> WeakReference<T> findRowAndReturnEntity (
            // название таблицы внутри Tablets
            @lombok.NonNull @com.typesafe.config.Optional final ObjectFromRowConvertInterface<T> entityToCassandraConverter,
            // название колонки
            @lombok.NonNull final String columnName,
            // параметр по которому введется поиск
            @lombok.NonNull @com.typesafe.config.Optional final U paramName
    ) {
        this.checkSessionNotClosed();

        return EntitiesInstances.generateWeakEntity(
                entityToCassandraConverter.generate().generate(
                        this.getRowFromTabletsKeyspace(
                                entityToCassandraConverter,
                                AnnotationInspector.getEntityPrimaryKey( entityToCassandraConverter )[0],
                                paramName
                        ).get()
                )
        );
    }

    @lombok.NonNull
    @org.jetbrains.annotations.Contract( value = "_, _ -> fail" )
    default <T, U> WeakReference<T> findRowAndReturnEntity (
            // название таблицы внутри Tablets
            @lombok.NonNull @com.typesafe.config.Optional final ObjectFromRowConvertInterface<T> entityToCassandraConverter,
            // параметр по которому введется поиск
            @lombok.NonNull @com.typesafe.config.Optional final U paramName
    ) {
        this.checkSessionNotClosed();

        return EntitiesInstances.generateWeakEntity(
                entityToCassandraConverter.generate().generate(
                        this.getRowFromTabletsKeyspace(
                                entityToCassandraConverter,
                                AnnotationInspector.getEntityPrimaryKey( entityToCassandraConverter )[0],
                                paramName
                        ).get()
                )
        );
    }

    @SuppressWarnings(
            value = "возвращает список значений из БД"
    )
    @lombok.NonNull
    @org.jetbrains.annotations.Contract( value = "_, _, _ -> !null" )
    default <T> WeakReference< List< Row > > getListOfEntities (
            // название таблицы внутри Tablets
            @lombok.NonNull @com.typesafe.config.Optional final EntityToCassandraConverter entityToCassandraConverter,
            // название колонки
            @lombok.NonNull final String columnName,
            // список параметрлв по которым введется поиск нескольких записей
            @lombok.NonNull final List< T > ids
    ) {
        return EntitiesInstances.generateWeakEntity();
    }

    @SuppressWarnings(
            value = "возвращает список значений из БД"
    )
    @lombok.NonNull
    @org.jetbrains.annotations.Contract( value = "_, _ -> !null" )
    default <T> WeakReference< List< Row > > getListOfEntities (
            // название таблицы внутри Tablets
            @lombok.NonNull @com.typesafe.config.Optional final EntityToCassandraConverter entityToCassandraConverter,
            // список параметрлв по которым введется поиск нескольких записей
            @lombok.NonNull final List< T > ids
    ) {
        return EntitiesInstances.generateWeakEntity();
    }

    @lombok.NonNull
    @org.jetbrains.annotations.Contract( value = "_ -> !null" )
    default ResultSet completeCommand (
            @lombok.NonNull final String query
    ) {
        this.checkSessionNotClosed();

        return this.getSession().execute(
                this.getSession().prepare( query ).bind()
        );
    }

    @lombok.NonNull
    @org.jetbrains.annotations.Contract( value = "_ -> !null" )
    default ResultSet completeCommand (
            @lombok.NonNull final BuildableQuery buildableQuery
    ) {
        this.checkSessionNotClosed();

        return this.getSession().execute(
                this.getSession().prepare( buildableQuery.asCql() ).bind()
        );
    }

    @lombok.NonNull
    @org.jetbrains.annotations.Contract( value = "_ -> !null" )
    default ResultSet completeCommand (
            @lombok.NonNull final StringBuilder stringBuilder
    ) {
        this.checkSessionNotClosed();

        return this.getSession().execute( stringBuilder.toString() );
    }

    @lombok.NonNull
    default Session getSession() {
        return CassandraDataControl.getInstance().getSession();
    }

    @lombok.NonNull
    default Cluster getCluster() {
        return CassandraDataControl.getInstance().getCluster();
    }

    @lombok.NonNull
    default CodecRegistry getCodecRegistry() {
        return CassandraDataControl.getInstance().getCodecRegistry();
    }
}
