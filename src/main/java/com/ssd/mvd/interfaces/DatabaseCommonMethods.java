package com.ssd.mvd.interfaces;

import com.ssd.mvd.database.CassandraDataControl;
import com.ssd.mvd.constants.CassandraCommands;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Row;

import java.text.MessageFormat;
import java.util.Collections;
import java.util.List;

/*
хранит все стандартные методы для сервисов работающих с БД
*/
public interface DatabaseCommonMethods {
    /*
    возвращает одно конкретное значение из БД
    */
    default <U> Row getRowFromTabletsKeyspace (
            // название таблицы внутри Tablets
            final EntityToCassandraConverter entityToCassandraConverter,
            // название колонки
            final String columnName,
            // параметр по которому введется поиск
            final U paramName
    ) {
        return this.getSession().execute(
                MessageFormat.format(
                        """
                        {0} {1}.{2} WHERE {3} = {4};
                        """,
                        CassandraCommands.SELECT_ALL,

                        entityToCassandraConverter.getEntityKeyspaceName(),
                        entityToCassandraConverter.getEntityTableName(),

                        columnName,
                        paramName
                )
        ).one();
    }

    default <T, U> T findRowAndReturnEntity (
            // название таблицы внутри Tablets
            final ObjectFromRowConvertInterface<T> entityToCassandraConverter,
            // название колонки
            final String columnName,
            // параметр по которому введется поиск
            final U paramName
    ) {
        return entityToCassandraConverter.generate(
                this.getRowFromTabletsKeyspace(
                        entityToCassandraConverter,
                        columnName,
                        paramName
                )
        );
    }

    /*
    возвращает список значений из БД
    */
    default <T> List< Row > getListOfEntities (
            // название таблицы внутри Tablets
            final EntityToCassandraConverter entityToCassandraConverter,
            // название колонки
            final String columnName,
            // список параметрлв по которым введется поиск нескольких записей
            final List< T > ids
    ) {
        return Collections.emptyList();
    }

    default Session getSession() {
        return CassandraDataControl.getInstance().getSession();
    }

    default Cluster getCluster() {
        return CassandraDataControl.getInstance().getCluster();
    }
}
