package com.ssd.mvd.interfaces;

import com.ssd.mvd.constants.CassandraTables;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Row;
import java.util.List;

/*
хранит все стандартные методы для сервисов работающих с БД
*/
public interface DatabaseCommonMethods {
    /*
    возвращает одно конкретное значение из БД
    */
    Row getRowFromTabletsKeyspace (
            // название таблицы внутри Tablets
            final CassandraTables cassandraTableName,
            // название колонки
            final String columnName,
            // параметр по которому введется поиск
            final String paramName
    );

    /*
    возвращает список значений из БД
    */
    <T> List< Row > getListOfEntities (
            // название таблицы внутри Tablets
            final CassandraTables cassandraTableName,
            // название колонки
            final String columnName,
            // список параметрлв по которым введется поиск нескольких записей
            final List< T > ids
    );

    Session getSession();
}
