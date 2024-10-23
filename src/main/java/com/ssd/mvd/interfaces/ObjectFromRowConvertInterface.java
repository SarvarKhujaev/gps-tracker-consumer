package com.ssd.mvd.interfaces;

import com.datastax.driver.core.GettableData;

@SuppressWarnings( value = "хранит методы для конвертации объекта при помощи ROW из БД" )
public interface ObjectFromRowConvertInterface<T> extends EntityToCassandraConverter {
    @lombok.NonNull
    T generate ( @lombok.NonNull final GettableData row );

    @lombok.NonNull
    ObjectFromRowConvertInterface<T> generate ();
}
