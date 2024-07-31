package com.ssd.mvd.interfaces;

import com.datastax.driver.core.Row;

/*
хранит методы для конвертации объекта при помощи ROW из БД
*/
public interface ObjectFromRowConvertInterface<T> extends EntityToCassandraConverter {
    T generate ( final Row row );

    ObjectFromRowConvertInterface<T> generate ();
}
